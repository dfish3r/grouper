//let memberAddData = {}; // returned from UiV2Group.viewHistoryChartResults
//let memberDeleteData = {}; // returned from UiV2Group.viewHistoryChartResults

function drawGraphModuleD3() {
    const c3_mshipAddData = {
        x: 'date',
        columns: [
            ['date', ...memberAddData['date'].map(dt => new Date(dt*1000))]
        ]
    }

    for (const [key, value] of Object.entries(memberAddData)) {
        if (key != 'date') {
            c3_mshipAddData.columns.push([key, ...value],)
        }
    }

    const c3_mshipDeleteData = {
        x: 'date',
        columns: [
            ['date', ...memberDeleteData['date'].map(dt => new Date(dt*1000))]
        ]
    }

    for (const [key, value] of Object.entries(memberDeleteData)) {
        if (key != 'date') {
            c3_mshipDeleteData.columns.push([key, ...value],)
        }
    }

    const chartAdds = c3.generate({
        bindto: '#mshipAddsChart',
        data: c3_mshipAddData,
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: '%Y-%m-%d %H:%M',
                    rotate: 90,
                    multiline: false
                }
            }
        }
    });

    const chartDeletes = c3.generate({
        bindto: '#mshipDeletesChart',
        data: c3_mshipDeleteData,
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: '%Y-%m-%d %H:%M',
                    rotate: 90,
                    multiline: false
                }
            }
        }
    });

}
