#!/usr/bin/env node
'use strict';
const path = require('node:path');
const fs = require('node:fs');
const util = require('node:util');
const {createRequire} = require('node:module');
const {pathToFileURL} = require('node:url');
// Preserve stdout for the Rust readiness protocol.
for (const key of ['log','info','debug','warn','error']) console[key] = (...args) => process.stderr.write(util.format(...args)+'\n');
const args = {};
for (let i=2;i<process.argv.length;i+=2) args[process.argv[i]] = process.argv[i+1];
const source = args['--source'];
const project = args['--project'];
if (!source || !path.isAbsolute(source) || !/^demo-[a-z0-9-]+$/.test(project || '')) throw Error('Require absolute --source and demo --project');
if (![18,20,22].includes(Number(process.versions.node.split('.')[0]))) throw Error('Supported Node major versions: 18, 20, 22');
process.env.GCLOUD_PROJECT = project;
process.env.GOOGLE_CLOUD_PROJECT = project;
process.env.FUNCTIONS_EMULATOR = 'true';
if (args['--target']) process.env.FUNCTION_TARGET = args['--target'];
const timeoutMs = Number(process.env.FIREBASE_EMU_FUNCTION_TIMEOUT_MS || 30000);
if (!Number.isFinite(timeoutMs) || timeoutMs < 1) throw Error('Invalid function timeout');
const express = require('express');
const app = express();
app.disable('x-powered-by');
const registry = new Map();
const supportedEvents = new Set(['providers/cloud.firestore/eventTypes/document.create','providers/cloud.firestore/eventTypes/document.update','providers/cloud.firestore/eventTypes/document.write','providers/cloud.firestore/eventTypes/document.delete','google.pubsub.topic.publish','google.storage.object.finalize','google.storage.object.delete']);
const deadline = (promise) => {
 let timer;
 return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Function execution timed out')),timeoutMs);})]).finally(()=>clearTimeout(timer));
};
function validRegions(value) {
 const regions = value?.regions ?? (value?.region ? [value.region] : ['us-central1']);
 if (!Array.isArray(regions) || !regions.length || regions.some(region=>typeof region !== 'string' || !/^[a-z]+(?:-[a-z0-9]+)+$/.test(region))) throw Error('Manifest regions must be non-empty Firebase region names');
 return regions;
}
function manifestTrigger(spec) {
 const trigger = spec?.trigger;
 if (!trigger || typeof trigger !== 'object' || typeof trigger.type !== 'string') throw Error('Manifest function requires trigger.type');
 if (trigger.type === 'http') return {httpsTrigger:{}};
 const requiredString = (key) => {
  if (typeof trigger[key] !== 'string' || !trigger[key]) throw Error(`Manifest ${trigger.type} trigger requires ${key}`);
  return trigger[key];
 };
 const definitions = {
  pubsub:()=>({eventType:'google.pubsub.topic.publish',resource:`projects/${project}/topics/${requiredString('topic')}`,service:'pubsub.googleapis.com'}),
  'storage.finalize':()=>({eventType:'google.storage.object.finalize',resource:`projects/_/buckets/${requiredString('bucket')}`,service:'storage.googleapis.com'}),
  'storage.delete':()=>({eventType:'google.storage.object.delete',resource:`projects/_/buckets/${requiredString('bucket')}`,service:'storage.googleapis.com'}),
 };
 if (!definitions[trigger.type]) throw Error(`Unsupported manifest trigger type: ${trigger.type}`);
 return {eventTrigger:definitions[trigger.type]()};
}
function resolveHandler(exports, name) {
 let value = exports;
 for (const part of name.split('.')) {
  if (!value || !Object.prototype.hasOwnProperty.call(value,part)) return undefined;
  value = value[part];
 }
 return value;
}
function loadManifest(exports) {
 const requestedPath = args['--manifest'] ? path.resolve(source,args['--manifest']) : path.join(source,'.firebase-emu-functions.json');
 if (!fs.existsSync(requestedPath)) {
  if (args['--manifest']) throw Error(`Functions manifest not found: ${requestedPath}`);
  return;
 }
 const sourceRoot = fs.realpathSync(source);
 const manifestPath = fs.realpathSync(requestedPath);
 if (manifestPath !== sourceRoot && !manifestPath.startsWith(sourceRoot + path.sep)) throw Error('Functions manifest must stay inside its source directory');
 const parsed = JSON.parse(fs.readFileSync(manifestPath,'utf8'));
 if (!parsed || !Array.isArray(parsed.functions)) throw Error('Functions manifest requires a functions array');
 for (const spec of parsed.functions) {
  if (!spec || typeof spec.name !== 'string' || !/^[a-zA-Z0-9_-]+$/.test(spec.name)) throw Error('Manifest function requires a valid name');
  if (typeof spec.handler !== 'string' || !/^[a-zA-Z0-9_$.-]+$/.test(spec.handler)) throw Error(`Manifest ${spec.name} requires a valid handler`);
  const fn = resolveHandler(exports,spec.handler);
  if (typeof fn !== 'function') throw Error(`Manifest handler not exported: ${spec.handler}`);
  if (registry.has(spec.name)) throw Error(`Duplicate function name: ${spec.name}`);
  registry.set(spec.name,{fn,name:spec.name,regions:validRegions(spec),trigger:manifestTrigger(spec)});
 }
}
async function main() {
 const entryRequire = createRequire(path.join(source,'package.json'));
 const pkg = JSON.parse(fs.readFileSync(path.join(source,'package.json'),'utf8'));
 const entry = path.resolve(source,pkg.main || 'index.js');
 let exports;
 try { exports = entryRequire(entry); }
 catch (error) { if (error?.code !== 'ERR_REQUIRE_ESM') throw error; exports = await import(pathToFileURL(entry)); }
 function discover(obj,prefix='') {
  for (const [key,value] of Object.entries(obj)) {
   const name = prefix ? `${prefix}-${key}` : key;
   if (typeof value === 'function' && value.__trigger) {
    const trigger = value.__trigger;
    if (!trigger.httpsTrigger && !supportedEvents.has(trigger.eventTrigger?.eventType)) throw Error(`Unsupported trigger ${name}: ${JSON.stringify(trigger)}`);
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) throw Error(`Invalid export name: ${name}`);
    registry.set(name,{fn:value,name,regions:trigger.regions || ['us-central1'],trigger});
   } else if(value && typeof value === 'object') discover(value,name);
  }
 }
 discover(exports);
 loadManifest(exports);
 if (args['--target']) {
  const item=registry.get(args['--target']);
  registry.clear();
  if (item) registry.set(item.name,item);
 }
 if (!registry.size) throw Error('No supported function exports found');
 app.get('/__/health', (_req,res)=>res.json({ready:true}));
 app.use(express.json({limit:'10mb',verify(req,_res,buf){req.rawBody=Buffer.from(buf);}}));
 app.use(express.urlencoded({extended:true,limit:'10mb',verify(req,_res,buf){req.rawBody=Buffer.from(buf);}}));
 app.use(express.raw({type:()=>true,limit:'10mb',verify(req,_res,buf){req.rawBody=Buffer.from(buf);}}));
 app.post('/__/events',async(req,res)=>{
  const body=req.body;
  const item=registry.get(body?.name);
  if (!item || !item.trigger.eventTrigger) return res.status(404).json({error:{message:'Event function not found'}});
  if (!body.event || typeof body.event !== 'object' || !body.event.context || typeof body.event.context !== 'object' || !Object.hasOwn(body.event,'data')) return res.status(400).json({error:{message:'Expected event.data and event.context'}});
  const context=body.event.context;
  if(typeof context.eventId!=='string' || typeof context.timestamp!=='string' || context.eventType!==item.trigger.eventTrigger.eventType) return res.status(400).json({error:{message:'Invalid event context'}});
  // v1 SDK wraps legacy resource strings. Our Rust protocol uses modern resource objects.
  if(context.eventType.startsWith('providers/cloud.firestore/eventTypes/')) context.eventType=context.eventType.replace('providers/cloud.firestore/eventTypes/','google.firestore.');
  try { const result=await deadline(Promise.resolve().then(()=>item.fn(body.event.data,context))); res.json({result:result ?? null}); }
  catch(err){console.error(err);res.status(500).json({error:{message:err.message || 'Handler failed'}});}
 });
 app.use('/invoke/:name',async(req,res)=>{
  const item=registry.get(req.params.name);
  if (!item?.trigger.httpsTrigger) return res.status(404).json({error:{message:'HTTP function not found'}});
  const timer=setTimeout(()=>{if(!res.headersSent)res.status(504).json({error:{message:'Function execution timed out'}});else res.destroy();},timeoutMs);
  res.once('close',()=>clearTimeout(timer));
  try {await Promise.resolve(item.fn(req,res));} catch(err){console.error(err);if(!res.headersSent)res.status(500).json({error:{message:'Internal Server Error'}});else res.destroy();}
 });
 app.use((err,req,res,next)=>{if(res.headersSent)return next(err);res.status(err.status || 500).json({error:{message:err.status===400?'Malformed request body':'Request failed'}});});
 const server=app.listen(0,'127.0.0.1',()=>process.stdout.write('FIREBASE_EMU_READY '+JSON.stringify({port:server.address().port,functions:[...registry.values()].map(({fn,...info})=>info)})+'\n'));
 server.requestTimeout=timeoutMs+1000;
 let closing=false;
 const shutdown=()=>{if(closing)return;closing=true;server.close(()=>process.exit(0));setTimeout(()=>{server.closeAllConnections();process.exit(0);},2000).unref();};
 process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);process.on('disconnect',shutdown);
}
main().catch(err=>{console.error(err);process.exitCode=1;});
