const functions=require('firebase-functions');
const fs=require('node:fs');
function record(name,value){
 if(process.env.FIXTURE_EVENT_LOG) fs.appendFileSync(process.env.FIXTURE_EVENT_LOG,JSON.stringify({name,value})+'\n');
 return value;
}
if(process.env.FIXTURE_PID_FILE) fs.writeFileSync(process.env.FIXTURE_PID_FILE,String(process.pid));
exports.echo=functions.https.onCall(async(data,context)=>({data,uid:context.auth?.uid || null}));
exports.fail=functions.https.onCall(()=>{throw new functions.https.HttpsError('invalid-argument','Fixture error',{reason:'test'});});
exports.reject=functions.https.onCall(async()=>{throw Error('private failure');});
exports.http=functions.https.onRequest(async(req,res)=>{await new Promise(r=>setTimeout(r,5));res.status(201).set('x-fixture','yes').json({method:req.method,path:req.path,query:req.query,body:req.body,raw:req.rawBody?.toString()});});
exports.write=functions.firestore.document('items/{itemId}').onWrite((change,ctx)=>record('write',{before:change.before.exists?change.before.data():null,after:change.after.exists?change.after.data():null,id:change.after.exists?change.after.id:change.before.id,param:ctx.params.itemId,eventId:ctx.eventId}));
exports.create=functions.firestore.document('items/{itemId}').onCreate((snap,ctx)=>record('create',{data:snap.data(),param:ctx.params.itemId,exists:snap.exists}));
exports.remove=functions.firestore.document('items/{itemId}').onDelete((snap,ctx)=>record('remove',{data:snap.data(),param:ctx.params.itemId,exists:snap.exists}));
exports.update=functions.firestore.document('items/{itemId}').onUpdate((change,ctx)=>record('update',{before:change.before.data(),after:change.after.data(),param:ctx.params.itemId}));
exports.asyncCreate=functions.firestore.document('async/{itemId}').onCreate(async(snap,ctx)=>{await new Promise(r=>setTimeout(r,75));return record('asyncCreate',{data:snap.data(),param:ctx.params.itemId});});
exports.eventFail=functions.firestore.document('bad/{id}').onWrite(async()=>{throw Error('event failure');});
exports.topic=functions.pubsub.topic('fixture-topic').onPublish(message=>record('topic',{json:message.json,attributes:message.attributes}));
exports.schedule=functions.pubsub.schedule('every 5 minutes').onRun(ctx=>record('schedule',{eventId:ctx.eventId}));
exports.finalize=functions.storage.object().onFinalize((object,ctx)=>record('finalize',{bucket:object.bucket,name:object.name,generation:object.generation,eventId:ctx.eventId}));
exports.storageDelete=functions.storage.object().onDelete(object=>record('storageDelete',{bucket:object.bucket,name:object.name}));
