const express = require('express');
const router = express.Router();

const snowflake = require('snowflake-sdk');

// Create the connection pool instance
const connectionPool = snowflake.createPool(
    // connection options
    {
      account: "lehngog-hc48192",  // ex acc url : https://lehngog-hc48192.snowflakecomputing.com
      username: "jhmun23216",
      password: "A-wufshjdsus23-A"
    },
    // pool options
    {
      max: 10, 
      min: 0   
    }
);

//Post Method
router.post('/post', async (req, res) => {

    let condition = '';

    const limit = req.body.hasOwnProperty("limit") && req.body.limit > 0 ? req.body.limit : 10;
    const date = req.body.hasOwnProperty("date") && req.body.date !== '' ? req.body.date : '';

    if (date.match(/^\d{4}-\d{2}-\d{2}$/) !== null) {
        condition += `CC_REC_START_DATE={d '${date}'}`;
    }

    if (condition !== '') {
        condition = 'where ' + condition;
    }

    connectionPool.use(async (clientConnection) =>
    {
        const statement = await clientConnection.execute({
            sqlText: `select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CALL_CENTER" ${condition} LIMIT ${limit}`,
            complete: function (err, stmt, rows)
            {
                var stream = statement.streamRows();
                stream.on('data', function (row)
                {
                    // res.status(200).json(row);
                });
                stream.on('end', function (row)
                {
                    res.status(200).json(rows);
                });
            }
        });
    });
})

module.exports = router;