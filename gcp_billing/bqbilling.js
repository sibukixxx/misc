var gcloud = require('gcloud')({
    projectId: 'sibukixxx-gae-project'
});
var gcs = gcloud.storage() ;
var bq = gcloud.bigquery() ;

exports.main = function(context, data){
    var bucket = gcs.bucket(data.bucket) ;
    var file = bucket.file(data.name) ;
    var buffer = new Buffer('') ;
    file.createReadStream()
        .on('data', function(chunk){
            buffer = Buffer.concat([buffer, chunk]) ;
        })
        .on('finish', function(){
            var jsonBilling = JSON.parse(buffer) ;
            execute(data.name, jsonBilling) ;
        });
    context.success() ;
};

function execute(fileName, jsonBilling){
    var fileDate = fileName.match(/\d{4}-(0[1-9]|1[0-2])/g) ;
    var tableName = fileDate.toString().replace(/-/g, '') ;

    var dataset = bq.dataset('{データセット名}') ;
    var targetTable = dataset.table(tableName) ;

    targetTable.exists(function(err, exists){
        if ( !exists ) {
            // TODO: テーブルがなければ新規に作る
            var options = {schema: {
                "fields": [
                    {
                        "type": "timestamp",
                        "mode": "required", 
                        "name": "StartTime"
                    }, 
                    {
                        "type": "timestamp",
                        "mode": "required", 
                        "name": "EndTime"
                    }, 
                    {
                        "type": "STRING", 
                        "mode": "required", 
                        "name": "ProjectId"
                    }, 
                    {
                        "type": "STRING", 
                        "mode": "required", 
                        "name": "Description"
                    }, 
                    {
                        "type": "FLOAT", 
                        "mode": "nullable", 
                        "name": "Debits"
                    }, 
                    {
                        "type": "STRING", 
                        "mode": "required", 
                        "name": "Currency"
                    }, 
                    {
                        "type": "FLOAT", 
                        "mode": "nullable", 
                        "name": "Amount"
                    }, 
                    {
                        "type": "STRING", 
                        "mode": "nullable", 
                        "name": "Unit"
                    }, 
                    {
                        "type": "STRING", 
                        "mode": "required", 
                        "name": "FileName"
                    }
                ]
            }};
            dataset.createTable(tableName, options, function(err, table, apiResponse){
                if ( err ) {
                    console.log('err: ', err) ;
                    console.log('apiResponse: ', apiResponse) ;
                    return ;
                }
                exportBigQuery(table, fileName, jsonBilling) ;
            });
        }
        else exportBigQuery(targetTable, fileName, jsonBilling) ;
    }) ;
}

function exportBigQuery(table, fileName, jsonBilling){
    var query = util.format('SELECT COUNT(*) AS count FROM [%s] WHERE FileName = "%s"', table.metadata.id, fileName) ;
    bq.query(query, function(err, rows, nextQuery){
        if ( err ) {
            console.log('err: ', err) ;
            return ;
        }
        else if ( rows[0].count > 0 ) return ;

        for ( var i = 0; i < jsonBilling.length; i++ ) {
            var item = jsonBilling[i] ;

            var stime = moment(item.startTime).utc().format('YYYY-MM-DDTHH:mm:ssZ') ;
            var etime = moment(item.endTime).utc().format('YYYY-MM-DDTHH:mm:ssZ') ;

            var row = {
                insertId: uuid.v4(), 
                json: {
                    StartTime: stime, 
                    EndTime: etime, 
                    ProjectId: item.projectId, 
                    Description: item.description, 
                    Debits: item.cost.amount, 
                    Currency: item.cost.currency, 
                    Amount: item.measurements[0].sum, 
                    Unit: item.measurements[0].unit, 
                    FileName: fileName
                }
            };
            var options = {
                raw: true, 
                skipInvalidRows: true
            };
            table.insert(row, options, function(err, insertErrors, apiResponse){
                if ( err ) {
                    console.log('err: ', err) ;
                    console.log('apiResponse: ', apiResponse) ;
                }
            }) ;
        }
    });
}
