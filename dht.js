/*jslint node: true */
'use strict';

require('nodetime').profile({
    accountKey: '90f7467fafd88d68dea0cc20e3abe9818aaa2eb0', 
    appName: 'DHT.js'
  });

var dgram = require('dgram'),
    fs = require('fs'),
    bencode = require('bencode'),
    crypto = require('crypto'),
    bigint = require('bigint'),
    when = require('when'),
    _ = require('lodash'),
    xor = require('xor'),
    util = require('util'),
    timeout = require('when/timeout'),
    readline = require('readline'),
    yargs = require('yargs');

var PORT = 6881;
var HOST = 'router.bittorrent.com';


function DHT(outputFile){
  var outputStream = fs.createWriteStream(outputFile);
  var self = this;
  var dht = {};
  var dhtArray = [];
  var querys = {};

  var infoHashes = [];
  var infoHashesInt = {};
  var backlog = {};

  //public
  this.id = crypto.pseudoRandomBytes(20);

  this.bootstrap = function(host, port){
    return timeout(1000, lookup(crypto.pseudoRandomBytes(20), {ip: host, port: port}));
  };

  var hashesPositionCounter = 0;
  this.findPeers2 = function(){
    hashesPositionCounter++;
    hashesPositionCounter %= infoHashes.length;

    var infoHash = infoHashes[hashesPositionCounter];
    var bi = backlog[infoHash];
    
    var node = getClosest2(infoHash);

    if(!node){
      log(util.format('%s empty DHT, swapping', infoHash.toString('hex')));
      swapHash(bi)
      return self.findPeers2();;
    }

    bi.outstanding++;
    var tid = crypto.pseudoRandomBytes(4);
    var lookupPromise = lookup(infoHash, node, tid);
    var timeoutPromise = timeout(1000, lookupPromise);

    lookupPromise.done(function(result){

      if(result.nodes){
        for(var i = 0; i < result.nodes.length; i++){
          var node = result.nodes[i];
          addPrivateNode(bi, node.id, node.ip, node.port);
        }
      }

      if(result.datapoints){
        var newDatapoints = _.difference(result.datapoints, bi.datapoints);
        bi.datapoints = bi.datapoints.concat(newDatapoints);

        if(newDatapoints.length > 0){
          bi.outstanding = 0;
          outputStream.write(newDatapoints.join('\n')+'\n');
        }

      }
    }, function(e){
      log(e);
    });

    /*timeoutPromise.then(function(){
      //console.error('Lookup succeeded: %s:%d', node.ip, node.port);
    }, function(){
      console.error('%s lookup failed', infoHash.toString('hex'));
    });*/

    timeoutPromise.finally(function(){        
      if(bi.outstanding > 512 && bi.inDHT){//bi.datapoints >= bi.datapointEstimate){
        swapHash(bi);
      }
      self.findPeers2();
    });
  };

  this.addHashes = function(backlogItems){
    log('Preparing Hashs');
    _.each(backlogItems, function(backlogItem){
      backlogItem.datapoints = [];
      backlog[backlogItem.hash] = backlogItem;
      backlogItem.inDHT = false;
      backlogItem.outstanding = 0;
      backlogItem.totalOutstanding = 0;
      backlogItem.dht = {};
      backlogItem.dhtArray = [];

      _.each(dhtArray, function(node){
        addPrivateNode(backlogItem, node.id, node.ip, node.port);
      });
    });

    _(backlog)
      .values()
      .reject('inDHT')
      .sortBy(function(bi){ bi.datapointEstimate - bi.datapoints.length})
      .slice(0, 10-infoHashes.length)
      .each(function(bi){
        bi.inDHT = true;
        infoHashes.push(bi.hash);
        infoHashesInt[bi.hash] = bigint.fromBuffer(bi.hash);
      });
  }
  
  var swapHash = function(bi){
    var nbi = _(backlog)
      .values()
      .reject('inDHT')
      .sortBy('totalOutstanding')
      .first();
          
    infoHashes[_.indexOf(infoHashes, bi.hash)] = nbi.hash;
    infoHashesInt[nbi.hash] = bigint.fromBuffer(nbi.hash);
    delete infoHashesInt[bi.hash];
    
    log(util.format('%s removed %d/%d o: %d', bi.hash.toString('hex').slice(0, 5), bi.datapoints.length, bi.datapointEstimate, bi.outstanding));
    log(util.format('%s added %d/%d to: %d', nbi.hash.toString('hex').slice(0, 5), nbi.datapoints.length, nbi.datapointEstimate, nbi.totalOutstanding));

    bi.inDHT = false;
    bi.totalOutstanding += bi.outstanding;
    bi.outstanding = 0;

    nbi.inDHT = true;
  }

  //private
  var status_lines = 0;
  var displayStatus = function(){    
    readline.moveCursor(process.stdout, 0, -status_lines);
    readline.clearScreenDown(process.stdout);
    
    console.log('\nStatus:')
    for(var i = 0; i < infoHashes.length; i++){
      var bi = backlog[infoHashes[i]];

      var completion = (bi.datapoints.length / bi.datapointEstimate * 100).toFixed(2);


      console.log('%s %d% (%d)', bi.hash.toString('hex').slice(0,5), completion, bi.datapoints.length);
    }
    var foundValues = _.reduce(_.pluck(backlog, 'datapoints'), function(sum, datapoints) {
      return sum + datapoints.length;
    }, 0);

    var estimatedValues = _.reduce(_.pluck(backlog, 'datapointEstimate'), function(sum, num) {
      return sum + num;
    });

    console.log('values: %d - estimation: %d', foundValues, estimatedValues);
    status_lines = infoHashes.length + 3;
  };

  var log = function(line){
    readline.moveCursor(process.stdout, 0, -status_lines);
    readline.clearScreenDown(process.stdout);
    console.log(line);
    status_lines = 0;
    displayStatus();
  }

  setInterval(displayStatus,200);

  var addNode = function(id, ip, port){
    //var profileStart = new Date().getTime();
    if(!dht[id]){
      var node = {
        id: id,
        idInt: bigint.fromBuffer(id),
        ip: ip,
        port: port,
        queried: {},
        failures: 0,
        distances: {},

      };

      dht[id] = node;
      dhtArray.push(node);
    }
    //var profileDuration = new Date().getTime() - profileStart;
    //var timePerNode = (profileDuration / dhtArray.length).toFixed(2);
    //console.error('Profile: addNode %d ms %d ms/n %d (dht size)', profileDuration, timePerNode, dhtArray.length);
  };

  var addPrivateNode = function(bi, id, ip, port){
    if(!bi.dht[id]){
      var fullnode = {
        id: id,
        idInt: bigint.fromBuffer(id),
        ip: ip,
        port: port,
        queried: false,
        failures: 0
      };
      fullnode.distance = fullnode.idInt.xor(bigint.fromBuffer(bi.hash));

      bi.dht[id] = fullnode;
      bi.dhtArray.push(fullnode);    
    }
  }

  var getClosest2 = function(infoHash){
    var profileStart = new Date().getTime();

    var closestNode = undefined;
    var closestDistance = 0;
    var distanceCalculations = 0;
    var bi = backlog[infoHash];
  
    for(var i = 0; i < bi.dhtArray.length; i++){
      var node = bi.dhtArray[i];
      
      if(node.queried[infoHash]) continue;
      if(node.failures >= 3) continue;

      var distance = node.distance;
      
      if(!closestNode || distance.lt(closestDistance)){
        closestNode = node;
        closestDistance = distance
      }
    }

    var profileDuration = new Date().getTime() - profileStart;
    var timePerNode = (profileDuration / bi.dhtArray.length).toFixed(2);
    //console.error('Profile: getClosest2 %d ms %d ms/n %d (dht size) %d (distances calculated)', profileDuration, timePerNode, dhtArray.length, distanceCalculations);

    if(!closestNode){
      _(dhtArray)
        .sample(16)
        .each(function(node){
          addPrivateNode(bi, node.id, node.ip, node.port);
        });
      return null;
    }

    return closestNode;
  };

  var buildFindNodeQuery = function(infoHash, tid){
    return bencode.encode({
      t: tid,
      y: new Buffer('q'),
      q: new Buffer('find_node'),
      a: {
        id: self.id,
        target: infoHash
      }
    });
  };

  var buildGetPeersQuery = function(infoHash, tid){
    return bencode.encode({
      t: tid,
      y: new Buffer('q'),
      q: new Buffer('get_peers'),
      a: {
        id: self.id,
        info_hash: infoHash
      }
    });
  };
  
  var lookup = function(infoHash, node, tid){
    //console.error('lookup %s from %s:%d', infoHash.toString('hex'), node.ip, node.port);
    if(!tid) tid = crypto.pseudoRandomBytes(4);

    var deferred = when.defer();
    var buf = buildGetPeersQuery(infoHash, tid);
    node.failures++;
    client.send(buf, 0, buf.length, node.port, node.ip, function(err/*, bytes*/) {
      if (err) return deferred.reject(err);
      querys[tid] = {
        deferred: deferred,
        infoHash: infoHash,
        node: node
      };
    });
    return deferred.promise;
  };

  var client = dgram.createSocket('udp4');

  client.on('message', function(messageBuffer, remote) {
    try{
      var message = bencode.decode(messageBuffer);
    }
    catch (e){
      log('Response poorly formated');
      return;
    }

    if(!message.t || message.t.length !== 4)
      return;

    if(!querys[message.t])
      return log(util.format('Message took too long:', message.t.toString('hex')));

    if(!message.r){
      if(message.e) log(util.format('Error %s', message.e[1].toString()));
      else log('Query contained no response');
      return;
    }

    var values = message.r.values;
    if(values){    
      var infoHash = querys[message.t].infoHash.toString('hex');
      //var datapoints = 0;

      var datapoints = [];
      for(var i = 0; i < values.length; i++){
        var peer = values[i];
        if(peer instanceof Buffer){
          var ip = peer[0] + '.' + peer[1] + '.' + peer[2] + '.' + peer[3];
          var port = peer.readUInt16BE(4);
          var id = util.format('%s,%s:%d', infoHash, ip, port);
          datapoints.push(id);

          //datapoints++;
        }
      }
      //outputStream.write(csvPeers);

      querys[message.t].deferred.resolve({
        datapoints: datapoints
      });
      //delete querys[message.t.readUInt32BE(0)];
      //return;
    }

    var nodesBuffer = message.r.nodes;
    if(!nodesBuffer){
      querys[message.t].deferred.reject(new Error('No nodes value'));
      //delete querys[message.t.readUInt32BE(0)];
      return;
    }

    var nodes = [];
    for(var i = 0; i < nodesBuffer.length; i += 26){
      var node = {
        id: nodesBuffer.slice(i, i+20),
        ip: num2dot(nodesBuffer.readUInt32BE(i+20)),
        port: nodesBuffer.readUInt16BE(i+24)
      }
      addNode(node.id, node.ip, node.port);
      nodes.push(node);
    }

    querys[message.t].node.failures--;
    querys[message.t].deferred.resolve({nodes: nodes});
    //delete querys[message.t];
  });
}

var argv = yargs.argv;

var jsonFile = fs.readFileSync(argv._[0]);
var movieInfo = JSON.parse(jsonFile);
movieInfo = _.filter(movieInfo);

var hashes = _.map(movieInfo, function(movie){ 
  return {
    hash: new Buffer(movie.hash, 'hex'),
    datapointEstimate: parseInt(movie.seeds) + parseInt(movie.leechers)
  }   
});

var dht = new DHT(argv.o);

function bootstrap(){
  dht.bootstrap(HOST, PORT).done(function(){
    console.log('DHT Bootstrapped');
    dht.addHashes(hashes);
    _.each(_.range(0, 96), function(){
      dht.findPeers2();
    });
  },
  function(){
    console.log('DHT Bootstrap failed');
    bootstrap();
  });
};

bootstrap();

function num2dot(num){
  var d = num%256;
  for (var i = 3; i > 0; i--){
    num = Math.floor(num/256);
    d = num%256 + '.' + d;
  }
  return d;
}

