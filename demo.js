const Scrapery = require('./index.js');
const cheerio = require('cheerio');

const url = 'http://localhost:3000/';
let s = new Scrapery({ignore_robottxt: true, spoof: 'firefox'});

s.request(url, '', loadPages, err => console.log(err)).post_process((key, data) => data).sqlite("test.db", {name: "test", fields: ["title"]}); //.write('result.json');

function loadPages(html) {
    const $ = cheerio.load(html);
    $('ul li a').each((i, el) => {
        s.request(url + $(el).attr('href'), $(el).attr('href'), loadData, err => console.log(err));
    });
}

function loadData(html, data) {
    const $ = cheerio.load(html);
    data({ title: $('h1').text()});
}