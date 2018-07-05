const express = require('express')
const app = express()
const morgan = require('morgan')
const bodyParser = require('body-parser')
const mongoose = require('mongoose')
const keys = require('./config/keys')
const port = 3000
const cassandra = require('cassandra-driver')
const redis = require('redis')
const {Client} = require('pg')
const router = express.Router()
const User = require('./models/user_mongo')

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

app.use(bodyParser.urlencoded({extended: false}))
app.use(bodyParser.json())
app.use(morgan('dev'))


router.post('/', (req,res)=>{
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
})

//get user by id
router.get('/:id', (req,res)=>{
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
})

app.use('/api', router)
app.get('/', (req,res)=>{
    res.send('homepage text')
})

app.listen(port)
console.log(`server is running on ${port}`)


