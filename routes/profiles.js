const express = require('express')
const router = express.Router()
const mongoose = require('mongoose')
const cassandra = require('cassandra-driver')
const redis = require('redis')
const {Client} = require('pg')
const keys = require('../config/keys')
const User = require('../models/user_mongo')

//conecting to cassandra
const cassandraClient = new cassandra.Client({
    contactPoints: keys.cassandra.contactPoints,
    authProvider: new cassandra.auth.PlainTextAuthProvider(keys.cassandra.user, keys.cassandra.pwd)
});
cassandraClient.connect(err=>{
err ? console.log(`Cassandra connection error: ${err}`) :
console.log('Cassandra connected!')
});

//connecting to mongoDB
mongoose.connect(keys.mongo, err=>{
err ? console.log(`Mongo connection error: ${err}`) :
console.log('MongoDb connected!')
});

//connecting to redis
const redisClient = redis.createClient({db: keys.redis.db, password: keys.redis.pwd})
redisClient.on('error', err=>{
console.log(`Redis connection failed: ${err}`)
})
redisClient.on('connect', ()=>{
console.log('Redis connected!')
})

//connecting to postgres
const postgresClient = new Client(keys.postgres)
postgresClient.connect((err)=>{
err ? console.log(`Postgres connection error: ${err}`) :
console.log('Postgres connected!')
})

router.post('/add', (req,res)=>{
    if(req.body.username && req.body.email) {
        const index = redisClient.keys('*', (err, keys)=>{
            if (err) return console.log(err);
            
            let query = ''
            switch(keys.length % 3) {
                case 0:  //insert user to mongo
                    redisClient.set(keys.length, 'mongodb')
                    const newUser = new User({
                        id: keys.length,
                        username: req.body.username,
                        email: req.body.email
                    })
                    newUser.save(err=>{
                        if(err) {
                            console.log(err, 'while inserting into mongodb')
                            return res.json({success: false, message: 'An error has been occured while inserting in mongoDb'})
                        }
                        res.json({success: true, message: 'Succesfully inserted user to mongoDb'})
                    })
                    break
                case 1:  //insert user to cassandra
                    redisClient.set(keys.length, 'cassandra')
                    query = `INSERT INTO users.profiles (id, username, email) VALUES (${keys.length}, '${req.body.username}', '${req.body.email}')`
                    cassandraClient.execute(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while inserting into cassandra')
                            return res.json({success: false, message: 'An error has been occured while inserting in Cassandra'})
                        }
                        res.json({success: true, message: 'Succesfully inserted user to Cassandra'})
                    });
                    break
                case 2: //insert user to postgres
                    redisClient.set(keys.length, 'postgres')
                    query = `INSERT INTO profiles (id, username, email) VALUES ('${keys.length}', '${req.body.username}', '${req.body.email}');`
                    postgresClient.query(query, (err)=>{
                        if(err) {
                            console.log(err, 'while inserting into postgres')
                            return res.json({success: false, message: 'An error has been occured while inserting in Postgres'})
                        }
                        res.json({success: true, message: 'Succesfully inserted user to Postgres'})
                    })
                    break
                default:
                    //console.log(res)
    
                    break;
              }
            return keys.length;       
        });
    } else {
        res.send('Pls enter username and email!')
    }
    
})

//get user by id
router.get('/users/:id', (req,res)=>{
    const index = redisClient.keys('*', (err, keys)=>{
        if(req.params.id>=0 && req.params.id<=keys.length) {
            let query = ''
            switch(req.params.id % 3) {
                case 0: //selecting user from mongodb
                    User.findOne({ id: req.params.id }, (err, result)=>{
                        if(err) {
                            console.log(err, 'while retrieving from mongodb')
                            return res.json({success: false, message: 'An error has been occured while retrieving from MongoDb'})
                        }
                        res.json({  success: true, 
                                    message: `User selected from mongo`,
                                    user: result 
                                })
                    })
                    break
                case 1: //selecting user from cassandra
                    query = `SELECT id, username, email FROM users.profiles WHERE id = ${req.params.id}`
                    cassandraClient.execute(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while selecting from cassandra')
                            return res.json({success: false, message: 'An error has been occured while selecting from Cassandra'})
                        }
                        res.json({  success: true, 
                                    message: `User selected from Cassandra`,
                                    user: result.rows[0]
                                })
                    });
                    break
                case 2: //selecting user from postgres
                    query = `SELECT id, username, email FROM profiles WHERE id = ${req.params.id}`
                    postgresClient.query(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while selecting from Postgres')
                            return res.json({success: false, message: 'An error has been occured while selecting from Postgres'})
                        }
                        res.json({  success: true, 
                                    message: `User selected from Postgres`,
                                    user: result.rows[0]
                                })
                    });
                    break
            }
        }else{
            res.send(`This id doesn't exist`)
        }
    
    })
    
})

//get all users
router.get('/users', (req,res)=>{
    User.find((err, mongoUsers)=>{ //get all users from mongo
        if(err) {
            console.log(err, 'while retrieving from mongodb')
            res.json({success: false, message: 'An error has been occured while retrieving from MongoDb'})
        }
        const cassandraQuery = `SELECT * FROM users.profiles` //get all users from Cassandra
        cassandraClient.execute(cassandraQuery, (err,cassandraUsers)=>{
            if(err){
                console.log(err, 'while selecting from cassandra')
                res.json({success: false, message: 'An error has been occured while selecting from Cassandra'})
            }
            const psqlQuery = `SELECT * FROM profiles` //get all users from PSQL
            postgresClient.query(psqlQuery, (err,psqlUsers)=>{
                if(err){
                    console.log(err, 'while selecting from Postgres')
                    res.json({success: false, message: 'An error has been occured while selecting from Postgres'})
                }
                const allUsers = [...mongoUsers, ...cassandraUsers.rows, ...psqlUsers.rows].sort((a,b)=>{
                    return a.id-b.id
                })
                res.json({  success: true, 
                            message: `User selected from 3 dbs`,
                            allUsers: allUsers
                            
                        })
                
            });
        });
    }) 

})

//update user by id
router.put('/', (req,res)=>{ 

})

//search user
router.get('/search', (req,res)=>{
    const searchParam = req.query.query
    console.log(searchParam, 'to find') //full text search from mongo
    User.find({ $text: { $search: searchParam}}, (err, mongoResult)=>{
        if(err){
            console.log(err, 'while searching in mongo')
            res.json({success: false, message: `An error has been occured during MongoDB search`})
        }
        const cassandraQuery = `SELECT * FROM users.profiles` //get all users from Cassandra
        cassandraClient.execute(cassandraQuery, (err,cassandraUsers)=>{
            if(err){
                console.log(err, 'while selecting from cassandra')
                res.json({success: false, message: 'An error has been occured during Cassandra search'})
            }
            const cassandraResult = cassandraUsers.rows.filter(user => user.username == searchParam || user.email == searchParam)
            const psqlQuery = `SELECT * FROM profiles WHERE make_tsvector(username, email) @@ to_tsquery('${searchParam}');` //full text search from PSQL
            postgresClient.query(psqlQuery, (err,psqlSearch)=>{
                if(err){
                    console.log(err, 'while searching Postgres in')
                    res.json({success: false, message: 'An error has been occured during Postgres search'})
                }
                const foundUsers = [...mongoResult, ...cassandraResult, ...psqlSearch.rows].sort((a,b)=>{
                    return a.id-b.id
                }) 
                console.log(foundUsers.length, "length of ur pipirka")  
                if (foundUsers.length == 0) { //cheking if there is a search result
                    res.json({
                        success: true,
                        message: 'nothing was found'
                    })
                } else {
                    res.json({  success: true, 
                                message: 'Users matching ur request from 3dbs',
                                foundUsers: foundUsers
                            })
                }
            })
            
            
        })
    })

    ///res.send('pishov na hui!!')
})

module.exports = router