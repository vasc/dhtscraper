/*jslint node: true */
'use strict';

var dgram = require('dgram'),
    fs = require('fs'),
    bencode = require('bencode'),
    crypto = require('crypto'),
    bigint = require('bigint'),
    when = require('when'),
    _ = require('lodash'),
    xor = require('xor'),
    util = require('util'),
    timeout = require('when/timeout');

var PORT = 6881;
var HOST = 'router.bittorrent.com';


function DHT(){
  var self = this;
  var dht = {};
  var querys = {};

  //public
  this.id = crypto.pseudoRandomBytes(20);

  this.bootstrap = function(host, port){
    return lookup(crypto.pseudoRandomBytes(20), {ip: host, port: port});
  };

  this.findPeers = function(infoHash){

    var nodes = getClosest(infoHash, 1);
    //console.error(nodes);
    if(nodes.length === 0) {
      console.error('Empty DHT for', infoHash);
      setTimeout(function(){
        self.findPeers(infoHash);
      }, 10000);
      return;
    }
    var lookups = _.map(nodes, function(node){
      node.queried[infoHash] = true;
      var tid = crypto.pseudoRandomBytes(4);
      var promise = timeout(30000, lookup(infoHash, node, tid))
      promise.finally(function(){
        delete querys[tid.readUInt32BE(0)];
      })
      return promise;
    });

    when.any(lookups).then(function(){
      console.error('SOME Worked');
      self.findPeers(infoHash);
    }, function(){
      console.error('ALL Failed');
      self.findPeers(infoHash);
    });
  };
  
  //private
  var addNode = function(id, ip, port){
    if(!dht[id])
      dht[id] = {
        id: id,
        ip: ip,
        port: port,
        queried: {},
        failures: 0
      };
  };

  var getClosest = function(infoHash, maxNum){
    console.error('DHT size:', _.values(dht).length);
    console.error('DHT usable:', _(dht).values().reject(function(node){return node.failures>=3;}).value().length);
    return _(dht)
      .values()
      //.each(function(node){console.log(node);})
      .reject(function(node){ return node.queried[infoHash];})
      .reject(function(node){ return node.failures >= 3;})
      .map(function(node){
        return {
          node: node,
          distance: bigint.fromBuffer(xor(node.id, infoHash))
        };
      })
      .sort(function(a, b) {
        if (a.distance.lt(b.distance)) return -1;
        if (a.distance.gt(b.distance)) return 1;
        return 0;
      })
      //.each(function(node){console.log(node);})
      .slice(0, maxNum)
      .pluck('node')
      .value();
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
      querys[tid.readUInt32BE(0)] = {
        deferred: deferred,
        infoHash: infoHash,
        node: node
      };
      console.error('Query Buffer:', _.values(querys).length);
      //console.error('UDP message sent to ' + node.ip +':'+ node.port);
    });
    return deferred.promise;
  };

  var client = dgram.createSocket('udp4');

  client.on('message', function(messageBuffer, remote) {
    console.error('Message from %s:%d', remote.address, remote.port);

    try{
      var message = bencode.decode(messageBuffer);
    }
    catch (e){
      console.error('Response poorly formated');
      return;
    }

    if(!message.t || message.t.length !== 4)
      return;

    if(!querys[message.t.readUInt32BE(0)])
      return console.error('Message took too long:', message.t.toString('hex'));

    if(!message.r){
      //querys[message.t.readUInt32BE(0)].reject(new Error('Query contained no response'));
      if(message.e) console.error('Error', message.e[1].toString());
      else console.error('Query contained no response');
      return;
    }
    //console.log(util.inspect(message));
    var infoHash = querys[message.t.readUInt32BE(0)].infoHash.toString('hex');

    var values = message.r.values;
    if(values){
      _.each(values, function(peer){
        if(peer instanceof Buffer){
          var ip = peer.readUInt32BE(0);
          var port = peer.readUInt16BE(4);
          
          console.log('%s,%s:%d', infoHash, num2dot(ip), port);
        }
      });
      querys[message.t.readUInt32BE(0)].node.failures--;
      querys[message.t.readUInt32BE(0)].deferred.resolve();
      //delete querys[message.t.readUInt32BE(0)];
      return;
    }

    var nodes = message.r.nodes;
    if(!nodes){
      querys[message.t.readUInt32BE(0)].deferred.reject(new Error('No nodes value'));
      //delete querys[message.t.readUInt32BE(0)];
      return;
    }

    for(var i = 0; i < nodes.length; i += 26){
      var id = nodes.slice(i, i+20);
      var ip = nodes.readUInt32BE(i+20);
      var port = nodes.readUInt16BE(i+24);
      //console.error("%s:%d", num2dot(ip), port);
      addNode(id, num2dot(ip), port);
    }

    querys[message.t.readUInt32BE(0)].node.failures--;
    querys[message.t.readUInt32BE(0)].deferred.resolve();
    //delete querys[message.t.readUInt32BE(0)];
  });
}

/*var hashes = [
  new Buffer( '4B642D022980E5EBAA7CF4B6E1CC93769921CB42', 'hex'),
  new Buffer('65548EDADF2AD10F73DB8DFA002A80EFD2A1AB45', 'hex'),
  new Buffer('CE9FBDAA734CFBC160E8EF9D29072646C09958DD', 'hex'),
  new Buffer('4956A4E976EA948025C3C3554567CA2820F65F64', 'hex'),
  new Buffer('AAD050EE1BB22E196939547B134535824DABF0CE', 'hex'),
  new Buffer('803F725EED41FA46FD229D4A3DB47CC32B7B6BDE', 'hex'),
  new Buffer('80B0F2788FD33CBB6272049EDFE3911F6AEAE5AC', 'hex'),
  new Buffer('10AB9CAD41F545893AF00993CBFA168FABD46395', 'hex'),
  new Buffer('3535A1A6375A619507141C7975611740455E1954', 'hex'),
];*/

var jsonFile = fs.readFileSync(process.argv[2]);
var movieInfo = JSON.parse(jsonFile);

var hashes = _.map(_.pluck(movieInfo, 'hash'), function(hex){ return new Buffer(hex, 'hex')});

var dht = new DHT();
dht.bootstrap(HOST, PORT).done(function(){
  _.each(hashes, function(info_hash){
    dht.findPeers(info_hash);
  });
});

function num2dot(num){
  var d = num%256;
  for (var i = 3; i > 0; i--){
    num = Math.floor(num/256);
    d = num%256 + '.' + d;
  }
  return d;
}

