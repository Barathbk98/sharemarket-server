const express = require('express');
const cors = require('cors');
const axios = require('axios');
const Pool = require('pg').Pool;
const elasticsearch = require('elasticsearch');
var lineReader = require('line-reader');
const moment =require('moment')
const pgp = require('pg-promise')({
    capSQL: true
})


var split = require('split');
var through = require('through2');
var cron = require('node-cron');
var log = "../../../log/nginx/node-app.access.log"       //production
// var log = "./node-app.access.log.1"      //development
var lineno = "./lineno.txt"
const fs = require('fs')
const grok = require('grok-js')

var logger = require('logger').createLogger('request.log'); 
const port = 5000;
const db = pgp("postgres://bk:bk191998@sharemarket.cbkmk6wdlemj.us-east-1.rds.amazonaws.com:5432/sharemarket");

var client = new elasticsearch.Client({
    hosts:['http://localhost:9200']
})

client.cluster.health({},function(err,resp,status) {  
    console.log("-- Client Health --",resp);
  });

const pool = new Pool({
  user: 'bk',
  host: 'sharemarket.cbkmk6wdlemj.us-east-1.rds.amazonaws.com',
  database: 'sharemarket',
  password: 'bk191998',
  port: 5432,
})

const app = express();

app.use(cors())
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.post("/api/fetch",(req,res)=>{
    axios.post(`https://www.moneycontrol.com/mc/widget/basicchart/get_chart_value?classic=true&sc_did=${req.body.id}&dur=max`)
    .then(async (response)=>{
        let available = await pool.query(`SELECT * FROM sharedata WHERE shareid = '${req.body.id}'`)
        let values = [];
        let obj = [];
        const cs = new pgp.helpers.ColumnSet(['shareid', 'date', 'open' ,'high' ,'low' ,'close' ,'volume' ,'value'], {table: 'sharedata'});

        response.data.g1.map((data)=>{
            obj.push({index:{_index: 'sharemarket', _type: 'posts'}})
            obj.push({"shareid":`${req.body.id}`,"date":`${data.date}`, "open":`${data.open}`,"high":`${data.high}`,"low" : `${data.low}`,"close":`${data.close}`,"volume":`${data.volume}`,"value":`${data.value}`})

            values = [...values,{shareid:`${req.body.id}`,date:`${data.date}`, open:`${data.open}`,high:`${data.high}`,low : `${data.low}`,close:`${data.close}`,volume:`${data.volume}`,value:`${data.value}`}]
        })

        const query = pgp.helpers.insert(values, cs);
    if(available.rowCount == 0){
            db.none(query)
        .then(data => {
            client.bulk({
                index: "sharemarket",
                body: obj
            })

        })
        .catch(error => {
            res.json({success:false})
            console.log(error)
            // error;
        });
    }
    res.json({success : true , data : response.data.g1})

    //             pool.query(`INSERT INTO sharedata (shareid,date,open,high,low,close,volume,value) VALUES ('${req.body.id}','${data.date}','${data.open}','${data.high}','${data.low}','${data.close}','${data.volume}','${data.value}') RETURNING *`)
    //         .then((value)=>{
    //             client.index({
    //                 index: 'sharemarket',
    //                 id: `${value.rows[0].id}`,
    //                 type: 'posts',
    //                 body: {
    //                     "shareid": `${req.body.id}`,
    //                     "date": `${data.date}`,
    //                     "open": `${value.rows[0].open}`,
    //                     "high": `${value.rows[0].high}`,
    //                     "low": `${value.rows[0].low}`,
    //                     "close": `${value.rows[0].close}`,
    //                     "volume": `${value.rows[0].volume}`,
    //                     "value": `${value.rows[0].value}`
    //                 }
    //             }, function(err, resp, status) {
    //                 console.log(status);
    //             });
    //         })
    //         .catch((err)=>{
    //             console.log(err);
    //         })
    //         } 
    //     res.json({data : response.data.g1})
    // })
})
})

app.get("/api/getid",(req,res)=>{
    if(req.query.id != "")
    {
        pool.query(`SELECT DISTINCT shareid from sharedata WHERE shareid LIKE '${req.query.id}%'`)
        .then((response)=>{
            console.log(response.rows)
            res.json({data: response.rows});
        })
    } else {
        res.json ({data : []})
    }
})

app.get("/api/search",(req,res) => {
    from = req.query.size*req.query.from;
    var search = [];
    if(req.query.id != "" && req.query.value != "")
    {
    client.search({  
        index: 'sharemarket',
        type: 'posts',
        size: `${req.query.size}`,
        from: `${from}`,
        body: {
          query: {
              bool: {
                  must: [
                    {
                        match: { 
                            "shareid": `${req.query.id}`
                        }
                    },
                    {
                        match: {
                            "value" : `${req.query.value}`
                        }
                    }
                  ]
              }
          },
        }
      },function more(error, response,status) {
          if (error){
            console.log("search error: "+error)
          }
          else {
            response.hits.hits.forEach(function(hit){
                search.push(hit);
            });
            res.json({data : search})
          }
      });
    }
      else if(req.query.id != ""){
        client.search({  
            index: 'sharemarket',
            type: 'posts',
            size: `${req.query.size}`,
            from: `${from}`,
            body: {
              query: {
                  match: {
                    "shareid": `${req.query.id}`
                  }
              },
            }
          },function more(error, response,status) {
              if (error){
                console.log("search error: "+error)
              }
              else {
                response.hits.hits.forEach(function(hit){
                    search.push(hit);
                });
                res.json({data : search})
              }
          });
      } else {
        client.search({  
            index: 'sharemarket',
            type: 'posts',
            size: `${req.query.size}`,
            from: `${from}`,
            body: {
              query: {
                  match: {
                    "value": `${req.query.value}`
                  }
              },
            }
          },function more(error, response,status) {
              if (error){
                console.log("search error: "+error)
              }
              else {
                response.hits.hits.forEach(function(hit){
                    search.push(hit);
                });
                res.json({data : search})
              }
          });
      }
})

const p = '%{IP:client} - - \\[%{DATA:date}\\] "%{WORD:method} %{URIPATHPARAM:file} %{URIHOST:site}%{URIPATHPARAM:url}" %{INT:code} %{INT:request} "%{DATA:mtag}" "%{DATA:agent}"';
// const p = '%{DATA:log} \\[%{TIMESTAMP_ISO8601:timestamp}\\] %{URIPATHPARAM:request}';
// const str = 'info [2020-02-12T16:45:00.012Z] /api'
app.get("/api",(req,res)=>{
  
  // logger.format = function(level, date, message) {
  //   return "info" + " " + "["+date.toISOString()+"]" + " " + req.route.path;
  // };
  // logger.info();
  // console.log(req);
  res.send("This Server is Running !!!!");
})
let start="0";

// line_counter=(data)=>{
//   console.log(data);
  
// }

app.get('/api/logsearch',(req,res)=>{
  let offset = req.query.limit*req.query.offset;
  let search = [];
  client.search({
    index : "logs",
    type : "log",
    size : `${req.query.limit}`,
    // offset: `${offset}`,
      body : {
        query: {
          multi_match :{
            query : `${req.query.search}`,
            fields : ["client","method","code","agent"]
          }
        }
        
    }
  },(error,response,status) => {
    if (error){
      console.log("search error: "+error)
    }
    else {
      response.hits.hits.forEach(function(hit){
          search.push(hit);
      });
      res.json({data : search})
    }
  })
})

app.get("/api/logs",(req,res)=>{
  let from = req.query.limit*req.query.page;
  client.search({
    index: "logs",
    type: "log",
    size: `${req.query.limit}`,
    from: `${from}`
  },(error,response,status) => {
    if (error){
      console.log("search error: "+error)
    }
    else {
      // response.hits.hits.forEach(function(hit){
      //     search.push(hit);
      // });
      res.json({data : response.hits.hits})
    }
  })
})

app.get('/api/statuscount',(req,res) => {
  client.search({
    index : "logs",
    type : "log",
    size : "0",
    body:{
      aggs : {
        // match :{
        //   start:"0",
        //   code : "200"
        // },
        code_count : { terms : { field : "code" } },
        agent_count : { terms : { field : "agent" } },        
    }
    }
  },(error,response,status)=>{
    if(error){
      console.log(error)
    } else {
      res.json({data:response.aggregations})
    }
  })
})

app.get("/api/datehits",(req,res)=>{
  let enddate = moment().format("YYYY-MM-DDTHH:mm:ssZ");
  let startdate;
  let interval;
  let bucket;
  if(req.query.type==="day"){
    bucket = "24"
    interval = "1h";
    startdate = moment(enddate).subtract(1, 'days').format("YYYY-MM-DDTHH:mm:ss"); 
  } else if (req.query.type==="month") {
    bucket = "31"
    startdate = moment(enddate).subtract(1, 'months').format("YYYY-MM-DDTHH:mm:ss");
    interval = "1d"; 
  } else {
    startdate = moment(enddate).subtract(1, 'years').format("YYYY-MM-DDTHH:mm:ss"); 
  }
  console.log(startdate);
  console.log(enddate)

    client.search({
      index: "logs",
      type: "log",
      size : "0",
      body:{
          query:{
            bool : {
              filter : [{
                range : {
                  date : {
                    gte : `${startdate}`,
                    lte : `${enddate}`
                  }
                }
              }]
            }
          },  
          aggs:{
            hits: {
              auto_date_histogram : {
                field : "date",
                // calendar_interval : `${interval}`,
                buckets : `${bucket}`
              }
            }
          }
        }
    },(err,response,status)=>{
      if(err){
        console.log(err);
        res.status(400).send(err)
      } else {
        res.json({data : response.aggregations.hits.buckets , start : startdate , end : enddate})
      }
    })
})

cron.schedule('*/5 * * * * *', ()=>{
  start = fs.readFileSync(lineno,'UTF-8')
  console.log("line no ====>",parseInt(start))
  let elasticbuffer = [];

    grok.loadDefault((err,pattern) =>{
      if(err){
        console.log(err);
        return
      }
      const pat = pattern.createPattern(p);
      const reader = fs.createReadStream(log).pipe(split(/(\r?\n)/)).pipe(startAt(parseInt(start)));
    let current_line = parseInt(start);
      lineReader.eachLine(reader,(line, last) => {
        var str = line
        pat.parse(str, (err, obj) => {
          if (err) {
            console.error(err);
            return;
          }
          if(last){
            console.log("last");
            fs.writeFileSync(lineno,current_line)
            // bulkinsert(elasticbuffer);
            console.log("done");
          }
          if(obj!=null)
          {
            obj.date = moment(obj.date,'DD/MMM/YYYY:HH:mm:ss +0000')._d
            // elasticbuffer.push({index:{_index: 'logs', _type: 'log'}})
          // elasticbuffer.push(obj);
          client.index({
            index : "logs",
            type : "log",
            body : obj
          })
          // console.log(obj);
          }
        });
        current_line++;
        // console.log(line,last,current_line);
      })
      // bulkinsert(elasticbuffer);
  })
  
})

// function bulkinsert (elasticbuffer) {
//   client.bulk({
//     index: "logs",
//     body: elasticbuffer
// }).then(()=>{
//   console.log("inserted")
// }).catch(err=>{
//   console.log(err);
// })
// }

function startAt (nthLine) {
  var i = 0;
  nthLine = nthLine || 0;
  var stream = through(function (chunk, enc, next) {
    if (i>=nthLine){ 
      this.push(chunk)
    };
    if (chunk.toString().match(/(\r?\n)/)) i++;
    next();
  })
  return stream;
}


// const line_counter = ((i = parseInt(start)) => () => ++i)();

// line_counter = (start) =>{
//   i = parseInt(start)
// }


app.listen(port,()=>{
    console.log("listening on port",port);
})