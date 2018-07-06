const express = require('express')
const app = express()
const morgan = require('morgan')
const bodyParser = require('body-parser')
const port = 3000
const router = require('./routes/profiles')

app.use(bodyParser.urlencoded({extended: false}))
app.use(bodyParser.json())
app.use(morgan('dev'))

app.use('/api', router)
app.get('/', (req,res)=>{
    res.send('homepage text')
})

app.listen(port)
console.log(`server is running on ${port}`)
