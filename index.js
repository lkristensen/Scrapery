const axios = require('axios');
const https = require('https');
const fs = require('fs');

function padString(i) {
    return (i < 10) ? "0" + i : "" + i;
}

function prepDate(temp) {
    temp = temp || new Date();
    let d = temp.getFullYear().toString() + 
                 padString(temp.getMonth()) + 
                 padString(temp.getDate()) +
                 padString(temp.getHours());

    return d;
}

function prepCacheUrl(url) {
    url = new URL(url);
    const dir = url.host.replaceAll('.', '__').replaceAll(':', '__');
    const pathname = url.pathname.replaceAll('/', '__');
    const search = url.search.replaceAll('?', '__').replaceAll('&', '__');

    return './cache/' + dir +'_#_' + pathname + search;
}

function writeCache(url, html) {
    url = prepCacheUrl(url);
    const time = prepDate();

    fs.writeFile(url + '_#_' + time, html, (err) => {});
}

function parseRobotTxt(data) {
    let agents = {
        '*': []
    };
    let active_agent = '*'
    let lines = data.split('\n');
    lines.forEach(line => {
        line = line.split('#')[0];
        let [token, val] = line.split(':');
        if(token.trim() === 'User-agent') {
            active_agent = val.trim();
            if(!agents[active_agent]) {
                agents[active_agent] = [];
            }
        } else if (token.trim() === 'Disallow' || token.trim() === 'Allow' || token.trim() === 'Crawl-delay') {
            agents[active_agent].push({ action: token.trim(), val: val.trim() });
        }
    });
    return agents;
}

function allowedToCrawl(pathname, robot) {
    robot = robot['*'];
    let result = true;

    robot.filter(rule => rule.action === "Disallow" || rule.action === "Allow").forEach(rule => {
        let re = new RegExp("^"+rule.val);
        let match = re.test(pathname);
        if (match) {
            result = rule.action === "Disallow" ? false : true;
        }

    });

    return result;
}

function loadRobotTxt(that, origin, url, key, callback, error) {
    axios.get(origin + "/robots.txt").then(res => {
        const html = res.data;
        if(that.config.ignore_robottxt) {
            that.robots[origin] = {
                '*': []
            }                
        }
        else {
            that.robots[origin] = parseRobotTxt(html);
        }
        that.request(url, key, callback, error);
    }).catch(err => {
        that.robots[origin] = {
            '*': []
        }
        that.request(url, key, callback, error);
    });
}

function getCachedVersionPath(url) {
    const cacheUrl = prepCacheUrl(url).replace('./cache/', '');
    let files = fs.readdirSync("./cache");
    let str = '^' + cacheUrl + '_#_(\\d{10})$';
    let re = new RegExp(str);
    let match = files.filter(f => re.test(f));

    if (match.length > 0) {
        let d = match.map(m => m.match(re)[1]).sort((a, b) => parseInt(b) - parseInt(a));

        for (let i = d.length; i > 1; i--) {
            let da = d.pop();
            fs.unlink('./cache/' + cacheUrl + '_#_' + da, (err, res) => {});
        }
        return './cache/' + cacheUrl + '_#_' + d;
    }

    return false;
}

function Scrapery(config) {
    this.active_cached = 0;
    this.active_time = 0;
    this.throttled = [];
    this.data = {};
    this.file_path;
    this.loading = false;
    this.robots = {};
    this._print = false;
    this._pp = (key, data) => { return {key, ...data} };
    this.headers = {
        "Accept-Language": "*",
        "Accept-Encoding": "identity",
    }
    this.config = {
        concurrent_connections: 10,
        delay: 500,
        ignore_robottxt: false,
        cache: true,
        spoof: '',
        ...config
    }

    if (this.config.agent) {
        this.agent = this.config.agent
    } else {
        this.agent = new https.Agent({
            rejectUnauthorized: false
        });
    }
    if (!fs.existsSync('./cache/')) {
        fs.mkdirSync('./cache/');
    }
    if (this.config.spoof.toUpperCase() === "FIREFOX") {
        this.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0';
    } else if (this.config.spoof.toUpperCase() === "CHROME") {
        this.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36';
    }
}

Scrapery.prototype._submit_data = function (key, data) {
    if(this.data[key]) {
        this.data[key] = {
            ...this.data[key],
            ...data
        }
    } else {
        this.data[key] = data;
    }
};

Scrapery.prototype._complete = function () {
    let data = [];

    for(let d in this.data) {
        data.push(this._pp(d, this.data[d]));
    }

    if(this._print) {
        console.log(data);
    }
    if(this.file_path) {
        fs.writeFile(this.file_path, JSON.stringify(data), (err) => {
            if (err)
               console.log(err);
            else {
               console.log("File written successfully\n");
            }
        });
    }
}

Scrapery.prototype._return_html = function(key, callback, html) {
    let that = this;
    function data(data) {
        that._submit_data(key, data);
    }

    callback(html, data);
}

Scrapery.prototype._check_if_complete = function() {
    if(this.throttled.length === 0 && this.active_cached === 0 && !this.loading) {
        this._complete();
    }
} 

Scrapery.prototype._request = function(url, key, callback, error) {
    let that = this;

    axios.get(url, {
        httpsAgent: this.agent
    }).then(res => {
        const html = res.data;

        if (that.config.cache) {
            writeCache(url, html);
        }
        that._return_html(key, callback, html);
        that._check_if_complete();
    }).catch(err => {
        if(error) {
            error(err);
        }
        that._check_if_complete();
    });
}

Scrapery.prototype._process = function() {
    const dt = performance.now();
    const diff = dt - this.active_time;
    const spacing = this.config.delay / this.config.concurrent_connections;

    if (this.active_time === 0 || (diff > spacing && this.throttled.length > 0)) {
        this.active_time = dt;
        const d = this.throttled.shift();
        this._request(d.url, d.key, d.callback, d.error);
        setTimeout(this._process.bind(this), spacing);
    } else {
            if(this.throttled.length > 0) {
            setTimeout(this._process.bind(this), spacing - diff);
        }
    }
}

Scrapery.prototype.request = function(url, key, callback, error) {
    let u = new URL(url);

    if (!this.robots[u.origin]) {
        loadRobotTxt(this, u.origin, url, key, callback, error);
    } else {
        if (allowedToCrawl(u.pathname, this.robots[u.origin])) {
            let cachedPath = getCachedVersionPath(url)
            if (cachedPath) {
                this.active_cached++;
                fs.readFile(cachedPath, 'utf-8', (err, html) => {
                    if(!err) {
                        this._return_html(key, callback, html);
                        this.active_cached--;
                        this._check_if_complete();    
                    }
                });
            }
            else {
                this.throttled.push({url, key, callback, error});
                if (this.active_time === 0) {
                    this._process();
                }    
            }
        }
    }
    return this;
};

Scrapery.prototype.setHeader = function(header, val) {
    this.headers[header] = val;
    return this;
};

Scrapery.prototype.write = function(url) {
    this.file_path = url;
};

Scrapery.prototype.print = function() {
    this._print = true;
};

Scrapery.prototype.post_process = function(fn) {
    this._pp = fn;
    return this;
};

Scrapery.prototype.prep = function(fn) {
    this.loading = true;
    fn.apply();
    this.loading = false;
    return this;
}

module.exports = Scrapery;
