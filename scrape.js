var request = require('request'),
    zlib = require('zlib'),
    cheerio = require('cheerio'),
    _ = require('lodash');

var requestGz = function(url, callback){
  //console.log(url);
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
    callback(imdb);
  })
}

_.each(_.range(1, 401), function(pageNum){
  requestGz('http://kickass.to/movies/' + pageNum + '/', function(err, page){
    var $ = cheerio.load(page);
    var cells = $('.torrentnameCell');
    cells.each(function(){
      var url = $(this).find('.filmType').attr('href');
      var magnetLink = $(this).find('[title="Torrent magnet link"]').attr('href');

      if(url === undefined){
        console.error('shit', magnetLink);
        return;
      }

      imdbLink('http://kickass.to' + url, function(imdb){
        if(imdb && magnetLink){
          console.log(imdb + ','+ magnetLink);
        }
      });

    })
  });
});