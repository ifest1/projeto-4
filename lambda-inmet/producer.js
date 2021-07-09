const axios = require('axios')
const { Kinesis } = require("aws-sdk");

const kinesis = new Kinesis({region: 'us-east-1'});

module.exports.handler = async function handler() {
    try {
        const date = new Date(); 
        const day = date.getDate().toString();
        const year = date.getFullYear().toString()
        const month = (date.getMonth() + 1).toString()
        const fullDate = `${year}-${month}-${day}`
        let hour = date.getUTCHours().toString()

        if (hour.length < 2) {
            hour = "0" + hour + "00";
        }
        else {
            hour = hour + "00";
        }
        const response = await axios.get(
            `https://apitempo.inmet.gov.br/estacao/dados/${fullDate}/${hour}`
        )
        let filteredData = response.data.filter((item) => {
            if (item.UF == "PE") return item;
        })
        filteredData = filteredData.map(item => {
            return {
                Data: JSON.stringify({
                    'CODIGO_ESTACAO': item.CD_ESTACAO,
                    'DATA_MEDICAO': item.DT_MEDICAO,
                    'NOME_ESTACAO': item.DC_NOME,
                    'HORARIO_COLETA': item.HR_MEDICAO,
                    'LATITUDE': item.VL_LATITUDE,
                    'LONGITUDE': item.VL_LONGITUDE,
                    'PRECIPITACAO': item.PRE_INS,
                    'TEMPERATURA_MAXIMA': item.TEM_MAX,
                    'TEMPERATURA_MINIMA': item.TEM_MIN,
                    'RADIACAO_GLOBAL': item.RAD_GLO,
                    'PONTO_DE_ORVALHO': item.PTO_INS 
                }),
                PartitionKey: "1",
            };
        })

        await kinesis.putRecords({
            StreamName: 'inmetDataStream',
            Records: filteredData,
        }).promise();

        return {
            statusCode: 200,
            body: JSON.stringify(filteredData)
        }
    } catch(error) {
        console.log(error);
        return {
            statusCode: 500,
            body: JSON.stringify(error)
        }
    }
}
