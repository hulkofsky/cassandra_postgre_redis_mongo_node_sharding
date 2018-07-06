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
            if (err) return console.log(`An error has been occured while getting Redis keys ${err}`);
            
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
                        if(result) {
                            res.json({  success: true, 
                                message: `User selected from mongo`,
                                user: result 
                            })
                        }else{
                            res.json({  success: false, 
                                message: `user id not found in MongoDb`, 
                            })
                        }     
                    })
                    break
                case 1: //selecting user from cassandra
                    query = `SELECT id, username, email FROM users.profiles WHERE id = ${req.params.id}`
                    cassandraClient.execute(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while selecting from cassandra')
                            return res.json({success: false, message: 'An error has been occured while selecting from Cassandra'})
                        }
                        if(result.rows.length != 0) {
                            res.json({  success: true, 
                                message: `User selected from Cassandra`,
                                user: result.rows[0]
                            })
                        }else{
                            res.json({  success: false, 
                                message: `user id not found in Cassandra`,
                            })
                        }
                        
                    });
                    break
                case 2: //selecting user from postgres
                    query = `SELECT id, username, email FROM profiles WHERE id = ${req.params.id}`
                    postgresClient.query(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while selecting from Postgres')
                            return res.json({success: false, message: 'An error has been occured while selecting from Postgres'})
                        }
                        if(result.rows.length != 0) {
                            res.json({  success: true, 
                                message: `User selected from Postgres`,
                                user: result.rows[0]
                            })
                        }else{
                            res.json({  success: false, 
                                message: `user id not found in Postgres`,
                            })
                        }
                        
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
router.put('/users/:id', (req,res)=>{ 
    if(req.body.username && req.body.email) {
        const index = redisClient.keys('*', (err, keys)=>{
            if(req.params.id>=0 && req.params.id<=keys.length) {
                let query = ''
                switch(req.params.id % 3) {
                    case 0:
                        User.update({ id: req.params.id }, 
                                    { $set: {username: req.body.username, email: req.body.email}},
                                    (err, result)=>{
                                        if(err) {
                                            console.log(err, 'while updating user in mongodb')
                                            return res.json({success: false, message: 'An error has been occured while updating user in MongoDb'})
                                        }
                                        res.json({  success: true, 
                                                    message: `User ${req.params.id} successfully updated in MongoDb`,
                                                    user: result 
                                                })
                                    })
                        break
                    case 1:
                        query = `UPDATE users.profiles SET username='${req.body.username}',email='${req.body.email}' WHERE id=${req.params.id};`
                        cassandraClient.execute(query, (err,result)=>{
                            if(err){
                                console.log(err, 'while updating in cassandra')
                                return res.json({success: false, message: 'An error has been occured while updating in Cassandra'})
                            }
                            res.json({  success: true, 
                                        message: `User ${req.params.id} successfully updated in Cassandra`,
                                        user: result.rows
                                    })
                        });
                        break
                    case 2:
                        query = `UPDATE profiles SET username='${req.body.username}', email='${req.body.email}' WHERE id = ${req.params.id}`
                        postgresClient.query(query, (err,result)=>{
                            if(err){
                                console.log(err, 'while selecting from Postgres')
                                return res.json({success: false, message: 'An error has been occured while updating in Postgres'})
                            }
                            res.json({  success: true, 
                                        message: `User ${req.params.id} successfully updated in Postgres`,
                                        user: result.rows //empty array
                                    })
                        });
                        break
                    default:
                        break
                }
            } else{
                res.send('user id not fund')
            }
        })
    } else {
        res.send('Pls enter username and email to update')
    }
})

//delete user by id
router.delete('/users/:id', (req,res)=>{
    const index = redisClient.keys(`${req.params.id}`, (err, keys)=>{
        if(keys.length==1) {
            let query = ''
            redisClient.del(`${req.params.id}`,(err)=>{
                if (err)res.send(`error while deleting Redis key ${req.params.id}, ${err}`)
            })
            switch(req.params.id % 3) {
                case 0:
                    User.remove({ id: req.params.id }, (err, result)=>{
                        if(err) {
                            console.log(err, 'while updating user in mongodb')
                            return res.json({success: false, message: 'An error has been occured while deleting user from MongoDb'})
                        }
                        res.json({  success: true, 
                                    message: `User ${req.params.id} successfully deleted from MongoDb`, 
                                })
                    })
                    break
                case 1:
                    query = `DELETE from users.profiles WHERE id=${req.params.id};`
                    cassandraClient.execute(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while deleting from cassandra')
                            return res.json({success: false, message: 'An error has been occured while deleting from Cassandra'})
                        }
                        res.json({  success: true, 
                                    message: `User ${req.params.id} successfully deleted from Cassandra`,
                                })
                    });
                    break
                case 2:
                    query = `DELETE FROM profiles WHERE id = ${req.params.id}`
                    postgresClient.query(query, (err,result)=>{
                        if(err){
                            console.log(err, 'while deleting from Postgres')
                            return res.json({success: false, message: 'An error has been occured while deleting from Postgres'})
                        }
                        res.json({  success: true, 
                                    message: `User ${req.params.id} successfully deleted from Postgres`,
                                })
                    });
                    break
                default:
                    break
            }
        }else{
            res.send('user id not found')
        }
    })
})

//search user
router.get('/search', (req,res)=>{
    const searchParam = req.query.query
    //full text search from mongo
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
                    console.log(err, 'while searching in Postgres')
                    res.json({success: false, message: 'An error has been occured during Postgres search'})
                }
                const foundUsers = [...mongoResult, ...cassandraResult, ...psqlSearch.rows].sort((a,b)=>{
                    return a.id-b.id
                })   
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
})

module.exports = router