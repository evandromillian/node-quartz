'use strict';

let persistence = require('./lib/persistence');
let assert = require('assert');


let cli = persistence.createClient('quartz', 'root', '123456', '192.168.234.130', 3306, 'mysql');

cli.set('quartz:jobs:teste1', 
        JSON.stringify({ jobid: 'teste1' }), 
        '', 
        '', 
        0, 
        function(err, res) {
            assert.equal('Ok', res)

            cli.get('quartz:jobs:teste1', 
                    function(err, res) {
                        assert.equal(JSON.stringify({ jobid: 'teste1' }), res);
                    });
        });

cli.rpush('quartz:jobs', 
          JSON.stringify({ jobid: "teste2", cron: "10 0 0 0 0 0" }),
          function(err, res) {
              assert.equal(1, res);

              cli.rpush('quartz:jobs', 
                        JSON.stringify({ jobid: "teste3", cron: "0 30 0 0 0 0" }),
                        function(err, res) {
                            assert.equal(2, res);

                            cli.rpoplpush('quartz:jobs', 
                                            'quartz:processing', 
                                            function(err, res) { 
                                                assert.equal(JSON.stringify({ jobid: "teste3", cron: "0 30 0 0 0 0" }), res);

                                                cli.rpoplpush('quartz:jobs', 
                                                                'quartz:processing', 
                                                                function(err, res) { 
                                                                    assert.equal(JSON.stringify({ jobid: "teste2", cron: "10 0 0 0 0 0" }), res);

                                                                    cli.lrem('quartz:processing', 
                                                                                -1, 
                                                                                JSON.stringify({ jobid: "teste3", cron: "0 30 0 0 0 0" }), 
                                                                                function(err, res) { 
                                                                                    assert.equal(1, res);
                                                                                });
                                                                });
                                            });
                        });
          });