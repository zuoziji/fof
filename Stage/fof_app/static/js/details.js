/**
 * Created by root on 17-6-1.
 */


function ci_expo_earn_charts(capital_data) {
      var time_line = capital_data.day;
      var ci_expo_earn_line = echarts.init(document.getElementById('ci_expo_earn'), "shine");
      option = {
                    title: {
                        text: '行业敞口收益'
                    },
                    tooltip: {
                        trigger: 'axis',
                        formatter: function (params) {
                            console.log(params);
                            params = params[0];
                            return params.marker + params.value + "   " + params.name
                        },
                        axisPointer: {
                            animation: false
                        }
                    },
                    xAxis: {
                        type: 'category',
                        data:time_line,
                        splitLine:{
                                　　　　show:true
                                　　 }
                    },
                    yAxis: {
                        type: 'value',
                        boundaryGap: [0, '100%'],
                        splitLine: {
                            show: true
                        }
                    },
                    series: [{
                        name: '模拟数据',
                        type: 'line',
                        showSymbol: false,
                        hoverAnimation: false,
                        data: capital_data['ci_expo_earn']
                    }]
                };
     ci_expo_earn_line.setOption(option);
    }

function ci_expo_charts(capital_data) {
              var time_line = capital_data.day;
			  var ci_expo_line = echarts.init(document.getElementById('ci_expo_data'), "shine");
			  var key = capital_data.ci_expo.key;
			  var option = {
				title: {
				  text: '行业敞口'
				},
				tooltip : {
                        trigger: 'axis',
                        axisPointer: {
                            type: 'line'
                        }
                    },
                    legend: {
				  left:200,
                  width:600,
				  data: key
				},
                  grid:{
				    top:130
                  },
				toolbox: {
				  show: true,
				  feature: {
					magicType: {
					  show: true,
					  title: {
						stack: 'Stack'
					  },
					  type: ['stack']
					},
					saveAsImage: {
					  show: true,
					  title: "Save Image"
					}
				  }
				},
				calculable: true,
				xAxis: [{
				  type: 'category',
				  boundaryGap: false,
				  data: time_line,
                    splitLine:{
                                　　　　show:true
                                　　 }
				}],
				yAxis: [{
				  type: 'value'
				}],
				series: []
			  };
			  for(var i=0;i<capital_data.ci_expo.ci_expo.length;i++){
			      var obj = {};
			      obj['name'] = capital_data.ci_expo.ci_expo[i]['name'];
			      obj['type'] = 'bar';
			      obj['stack'] = "总量";
                  obj['smooth'] = true;
                  obj['itemStyle'] = {"normal":{"areaStyle":{"type":"default"}}};
                  obj['data'] = capital_data.ci_expo.ci_expo[i]['data'];
                  option.series.push(obj);
              }
             var ci_expo_bar = echarts.init(document.getElementById('ci_expo_pie'),"shine");
             var ci_bar = capital_data['ci_expo_pie'];
             var option1 = {
                    timeline: {
                        axisType: 'category',
                        data: time_line
                    },
                    options: [{
                            title: {
                                text: ci_bar[0]['name'],
                                left: 'center'
                            },
                            tooltip: {
                                trigger: 'axis'
                            },
                            calculable: true,
                            yAxis: [{
                                type: 'category',
                                axisLabel: {
                                    interval: 0,
                                 margin:10
                                },

                                data: key
                            }],
                            xAxis: [{
                                type: 'value'
                            }],
                            series: [{
                                type: 'bar',
                                data: ci_bar[0]['data']
                            }]
                        }
                    ]
                };
                 for(var i=1;i<ci_bar.length;i++){
                    var obj = new Object();
                    obj.title = {"text":ci_bar[i]['name']};
                    obj.series = [{'data':ci_bar[i]['data']}];
                    option1.options.push(obj);
                 }
                 ci_expo_line.setOption(option);
                 ci_expo_bar.setOption(option1);
}

function ci_dist_charts(capital_data) {
              var time_line = capital_data.day;
			  var ci_dist_line = echarts.init(document.getElementById('ci_dist_data'), "shine");
			  var key = capital_data.ci_dist.key;
			  var option = {
				title: {
				  text: '行业分布'
                },
				tooltip : {
                        trigger: 'axis',
                        axisPointer: {
                            type: 'line'
                        }
                    },
                    legend: {
				  left:200,
                  width:600,
				  data: key
				},
                  grid:{
				    top:130
                  },
				toolbox: {
				  show: true,
				  feature: {
					magicType: {
					  show: true,
					  title: {
						stack: 'Stack'
					  },
					  type: ['stack']
					},
					saveAsImage: {
					  show: true,
					  title: "Save Image"
					}
				  }
				},
				calculable: true,
				xAxis: [{
				  type: 'category',
				  boundaryGap: false,
				  data: time_line,
                    splitLine:{
                                　　　　show:true
                                　　 }
				}],
				yAxis: [{
				  type: 'value'
				}],
				series: []
			  };
			  for(var i=0;i<capital_data.ci_dist.ci_dist.length;i++){
			      var obj = {};
			      obj['name'] = capital_data.ci_dist.ci_dist[i]['name'];
			      obj['type'] = 'line';
			      obj['stack'] = "总量";
                  obj['smooth'] = true;
                  obj['itemStyle'] = {"normal":{"areaStyle":{"type":"default"}}};
                  obj['data'] = capital_data.ci_dist.ci_dist[i]['data'];
                  option.series.push(obj);
              }
             var ci_dist_pie = echarts.init(document.getElementById('ci_dist_pie'),"shine");
             option1 = {
                    timeline : {
                    data : time_line,
                    axisType: 'category',
                    label : {
                        formatter: function (s) {
                             var d = new Date(s);
                            var mon = d.getMonth() +1;
                            var tDate = mon  + '月' + d.getDate() + '日';
                            return tDate
                        }
                    },
                        tooltip : {
                            trigger: 'item',
                            alwaysShowContent:true,
                            formatter: '{b}'

                        }
                    },
                    options : []
                    };
                for(var i=0;i<time_line.length;i++){
                    var indexKey = time_line[i];
                    var data = capital_data['ci_dist_pie'][indexKey];
                    var newData = [];
                    $.each(data,function (k,v) {
                        var dx = {"name":k,"value":v};
                        newData.push(dx)
                    });
                    var op =  {
            title : {
                text: '日行业分布',
                subtext: ''
            },
            tooltip : {
                trigger: 'item',
                formatter:  "{a} <br/>{b} : {c} ({d}%)"
            },
            toolbox: {
                show : true,
                feature : {
                    mark : {show: true},
                    dataView : {show: true, readOnly: false},
                    magicType : {
                        show: true,
                        type: ['pie', 'funnel'],
                        option: {
                            funnel: {
                                x: '25%',
                                width: '50%',
                                funnelAlign: 'left',
                                max: 1700
                            }
                        }
                    },
                    restore : {show: true},
                    saveAsImage : {show: true}
                }
            }
        };
                     op.series = [{"name":"持仓权重","type":'pie',"data":newData}];
                     option1.options.push(op);
                }

             ci_dist_line.setOption(option);
             ci_dist_pie.setOption(option1);
			}

function factor_charts(capital_data) {
              var time_line = capital_data.day;
			  var factorLine = echarts.init(document.getElementById('factor_data'), "shine");
			  var key = capital_data.factor.key;

			  var option = {
				title: {
				  text: '因子分析'
				},
				tooltip: {
				  trigger: 'axis'
				},
				legend: {
				  left:200,
                  width:600,
				  data: key
				},

				toolbox: {
				  show: true,
				  feature: {
					magicType: {
					  show: true,
					  title: {
						line: 'Line',
						bar: 'Bar',
						stack: 'Stack',
						tiled: 'Tiled'
					  },
					  type: ['stack', 'bar', 'line', 'tiled']
					},
					restore: {
					  show: true,
					  title: "Restore"
					},
					saveAsImage: {
					  show: true,
					  title: "Save Image"
					}
				  }
				},
				calculable: true,
				xAxis: [{
				  type: 'category',
				  boundaryGap: false,
				  data: time_line,
                    splitLine:{
                                　　　　show:true
                                　　 }
				}],
				yAxis: [{
				  type: 'value'

				}],
				series: []
			  };
			  for(var i=0;i<capital_data.factor.factor.length;i++){
			      var obj = {};
			      obj['name'] = capital_data.factor.factor[i]['name'];
			      obj['type'] = 'line';
                  obj['smooth'] = true;
                  obj['data'] = capital_data.factor.factor[i]['data'];
                  option.series.push(obj);
              }
             factorLine.setOption(option);
			}

function capital_charts(capital_data) {
              var time_line = capital_data.day;
			  var capitalLine = echarts.init(document.getElementById('capital_data'), "shine");
			  var key = capital_data.capital.key;
			  console.log(key);
			  var option = {
				title: {
				  text: '敞口分析'
				},
				tooltip: {
				  trigger: 'axis'
				},
				legend: {
				  left:200,
                  width:600,
				  data: key
				},
				toolbox: {
				  show: true,
				  feature: {
					magicType: {
					  show: true,
					  title: {
						line: 'Line',
						bar: 'Bar',
						stack: 'Stack',
						tiled: 'Tiled'
					  },
					  type: ['stack', 'bar', 'line', 'tiled']
					},
					restore: {
					  show: true,
					  title: "Restore"
					},
					saveAsImage: {
					  show: true,
					  title: "Save Image"
					}
				  }
				},
				calculable: true,
				xAxis: [{
				  type: 'category',
				  boundaryGap: false,
				  data: time_line,
                splitLine:{
                            show:true
                }
				}],
				yAxis: [{
				  type: 'value'
				}],
				series: []
			  };
			  for(var i=0;i<capital_data.capital.capital.length;i++){
			      var obj = {};
			      obj['name'] = capital_data.capital.capital[i]['name'];
			      obj['type'] = 'line';
                  obj['smooth'] = true;
                  obj['itemStyle'] = {"normal":{"areaStyle":{"type":"default"}}};
                  obj['data'] = capital_data.capital.capital[i]['data'];
                  option.series.push(obj);
              }
             capitalLine.setOption(option);
			}

function tab5(capital_data) {
    $('#tab5').DataTable({
         "info":false,
              "paging": true,
                'data':capital_data['f_data'],
                             "language": {
      "emptyTable": "暂时没有找到可用的数据"
    },
                columns: [
                    { data: 'action' },
                     { data: 'data.Estimate'},
                     {data:'data.Std_Error'},
                    {data:'data.t_Value'},
                    {data:'data.Pr(>|t|)'}
                ],
                "columnDefs": [{
                    targets: "_all",
                    orderable: false
                }
                ]
            });
}



