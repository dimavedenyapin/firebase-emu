'use strict';
exports.api = async (req,res) => {
  await new Promise(resolve=>setTimeout(resolve,5));
  res.status(202).json({method:req.method,path:req.path,body:req.body,project:process.env.GCLOUD_PROJECT});
};
exports.nested = {
  api(req,res) { res.json({nested:true}); },
};
exports.topic = async (data,context) => ({data,eventType:context.eventType});
exports.storage = async (data,context) => ({name:data.name,bucket:data.bucket,eventType:context.eventType});
