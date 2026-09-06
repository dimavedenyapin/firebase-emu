'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {spawn}=require('node:child_process');
const path=require('node:path');

const adapter=path.resolve(__dirname,'../adapter.cjs');
const source=path.resolve(__dirname,'../fixtures/plain');
async function start(extraArgs=[]) {
 const child=spawn(process.execPath,[adapter,'--source',source,'--project','demo-functions',...extraArgs],{env:{PATH:process.env.PATH},stdio:['ignore','pipe','pipe']});
 let stderr='',stdout='';
 child.stderr.on('data',chunk=>stderr+=chunk);
 const ready=await new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>reject(Error(`readiness timeout: ${stderr}`)),10000);
  child.once('exit',code=>{clearTimeout(timer);reject(Error(`exit ${code}: ${stderr}`));});
  child.stdout.on('data',chunk=>{
   stdout+=chunk;
   const line=stdout.split('\n').find(value=>value.startsWith('FIREBASE_EMU_READY '));
   if(line){clearTimeout(timer);resolve(JSON.parse(line.slice(19)));}
  });
 });
 return {child,ready,base:`http://127.0.0.1:${ready.port}`};
}
async function stop(child){if(child.exitCode===null){child.kill('SIGTERM');await new Promise(resolve=>child.once('exit',resolve));}}

test('explicit manifest discovers plain HTTP and event handlers',async()=>{
 const {child,ready,base}=await start();
 try {
  assert.deepEqual(ready.functions.map(item=>item.name),['plainHttp','nestedHttp','plainTopic','plainStorage']);
  const http=ready.functions.find(item=>item.name==='plainHttp');
  assert.deepEqual(http.regions,['asia-southeast1']);
  assert.deepEqual(http.trigger,{httpsTrigger:{}});
  const topic=ready.functions.find(item=>item.name==='plainTopic');
  assert.equal(topic.trigger.eventTrigger.eventType,'google.pubsub.topic.publish');
  assert.equal(topic.trigger.eventTrigger.resource,'projects/demo-functions/topics/fixture-topic');
  const response=await fetch(base+'/invoke/plainHttp/tail',{method:'POST',headers:{'content-type':'application/json'},body:'{"safe":true}'});
  assert.equal(response.status,202);
  assert.deepEqual(await response.json(),{method:'POST',path:'/tail',body:{safe:true},project:'demo-functions'});
  assert.deepEqual(await (await fetch(base+'/invoke/nestedHttp')).json(),{nested:true});
  const invokeEvent=async(name,data)=>{
   const item=ready.functions.find(value=>value.name===name);
   const eventType=item.trigger.eventTrigger.eventType;
   const response=await fetch(base+'/__/events',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({name,event:{data,context:{eventId:'plain-1',timestamp:'2025-01-01T00:00:00.000Z',eventType,resource:{service:item.trigger.eventTrigger.service,name:item.trigger.eventTrigger.resource},params:{}}}})});
   assert.equal(response.status,200);
   return response.json();
  };
  const message={data:Buffer.from('{"plain":true}').toString('base64'),attributes:{source:'fixture'}};
  assert.deepEqual(await invokeEvent('plainTopic',message),{result:{data:message,eventType:'google.pubsub.topic.publish'}});
  assert.deepEqual(await invokeEvent('plainStorage',{name:'plain.txt',bucket:'demo-functions.appspot.com'}),{result:{name:'plain.txt',bucket:'demo-functions.appspot.com',eventType:'google.storage.object.finalize'}});
 } finally {await stop(child);}
});

test('--target exposes only the selected manifest function',async()=>{
 const {child,ready,base}=await start(['--target','nestedHttp']);
 try {
  assert.deepEqual(ready.functions.map(item=>item.name),['nestedHttp']);
  assert.equal((await fetch(base+'/invoke/plainHttp')).status,404);
  assert.equal((await fetch(base+'/invoke/nestedHttp')).status,200);
 } finally {await stop(child);}
});
