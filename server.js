const express = require('express');
const cors = require('cors');
const axios = require('axios');
const Pool = require('pg').Pool;
const elasticsearch = require('elasticsearch');
const pgp = require('pg-promise')({
    capSQL: true
})
const port = 5000;
const db = pgp("postgres://bk:bk191998@localhost:5432/sharemarket");

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
            console.log(error)
            // error;
        });
    }
    res.json({data : response.data.g1})

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

app.get("/api",(req,res)=>{
  res.send("This Server is Running !!!!");
})

app.listen(port,()=>{
    console.log("listening on port",port);
})