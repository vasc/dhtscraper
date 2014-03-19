'use strict';

var request = require('request'),
    zlib = require('zlib'),
    cheerio = require('cheerio'),
    _ = require('lodash'),
    when = require('when');

var requestGz = function(url, callback){
  var pageRequest = request(url)
  .pipe(zlib.createGunzip())

  var page = "";
  pageRequest.on('data', function(data){
    page += data;
  });

  pageRequest.on('end', function(){
    callback(null, page);
  });

  pageRequest.on('error', function(err){
    callback(err);
  })
}

var imdbLink = function(url, callback){
  requestGz(url, function(err, page){
    var $ = cheerio.load(page);
    var imdb = $('.dataList').children().first().children().eq(1).find('a').text();
    var quality = $('[itemprop=quality]').text();
    callback(imdb, quality);
  })
}

var pagesPromisses = _.map(_.range(1, 4), function(pageNum){
  var deferedPage = when.defer();
  requestGz('http://kickass.to/movies/' + pageNum + '/', function(err, page){
    var $ = cheerio.load(page);
    var cells = $('.torrentnameCell');
    var cellPromises = _.map(cells, function(cell){
      var deferedCell = when.defer();
      var name = $(cell).find('.torrentname a').eq(1).text();
      var url = $(cell).find('.torType').attr('href');
      var seeds = $(cell).parent().find('.green.center').text();
      var leechers = $(cell).parent().find('.red.center').text();
      var magnetLink = $(cell).find('[title="Torrent magnet link"]').attr('href');

      if(url === undefined){
        deferedCell.resolve();
        return;
      }

      imdbLink('http://kickass.to' + url, function(imdb, quality){
        if(imdb && magnetLink){
          console.error(name);
          deferedCell.resolve({
            name: name,
            url: url,
            imdb: imdb,
            quality: quality,
            hash: magnetLink.slice(20, 60),
            magnet: magnetLink,
            seeds: seeds,
            leechers: leechers
          });
        }
        deferedCell.resolve();
      });
      return deferedCell.promise;
    });
    deferedPage.resolve(when.all(cellPromises));
  });
  return deferedPage.promise;
});

when.all(pagesPromisses).done(function(values){
  console.error('done!');
  console.log(JSON.stringify(_.flatten(values, true)));
})
