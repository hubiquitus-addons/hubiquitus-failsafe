var program = require('commander');

program
  .option('-m, --mongodb [path]', 'Mongo url')
  .option('-a, --aid [id]', 'deadQueue actor id')
  .option('-c, --collection [collection]', 'Mongo collection')
  .parse(process.argv);

var opts = {};
opts.mongo = program.mongodb;
opts.deadQueue = program.aid;
opts.collection = program.collection;

var deadQueue = require('./deadQueue')(opts);
