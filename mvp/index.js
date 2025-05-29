// Инициализация БД: curl localhost:3000/init -X POST
// Ввод данных: curl localhost:3000/upload --header "Content-Type: application/json" --data '[{ "query": "школа рядом", "lat": 42.87461, "lng": 74.61223, "timestamp": "2025-05-27T12:00:00" }, { "query": "детский сад", "lat": 42.87468, "lng": 74.61228, "timestamp": "2025-05-27T12:05:00" }, { "query": "школа", "lat": 42.87001, "lng": 74.60001, "timestamp": "2025-05-27T12:10:00" }, { "query": "школа", "lat": 42.87005, "lng": 74.60007, "timestamp": "2025-05-27T12:20:00" }, { "query": "детсад", "lat": 42.86400, "lng": 74.59000, "timestamp": "2025-05-27T12:25:00" }]' -v
// Получение данных: curl 'localhost:3000/heatmap?name=foo' или curl 'localhost:3000/heatmap?name=foo&minLat=10.4&maxLng=300.075'

const PORT = 3000; // Порт API сервера
const PREC = 100; // Точность округления (100 это 2 знака после запятой)
const DB_NAME = './db.sqlite'; // База данных (':memory:' не сохраняется в памяти)

const sqlite3 = require('sqlite3').verbose();
const express = require('express')

const db = new sqlite3.Database(DB_NAME);
const app = express();

app.use(express.json());

app.post('/init', (req, res) => {
    db.exec(
        `
            CREATE TABLE names(
                key INTEGER PRIMARY KEY,
                name TEXT
            );

            CREATE TABLE locs(
                key INTEGER PRIMARY KEY,
                name INTEGER,
                lat INTEGER,
                lng INTEGER,
                int INTEGER,
                FOREIGN KEY(name) REFERENCES names(key)
            );
        `,
        function(err) {
            if (err) { console.error(err.message); return; };

            res.send('OK');
        }
    );
});

app.post('/upload', (req, res) => {
    function processRec(rec) {
        if (rec >= req.body.length) {
            res.send('OK');
            return;
        }

        const {query, lat, lng} = req.body[rec];

        function processLoc(name) {
            const latInt = Math.round(lat * PREC);
            const lngInt = Math.round(lng * PREC);

            db.get(`SELECT key FROM locs WHERE name = ${name} AND lat = ${latInt} AND lng = ${lngInt}`, function(err, row) {
                if (err) { console.error(err.message); return; };

                if (!!row) {
                    const key = row.key;

                    db.exec(`UPDATE locs SET int = int + 1 WHERE key = ${key}`, function(err) {
                        if (err) { console.error(err.message); return; };

                        processRec(rec + 1);
                    });
                } else {
                    db.run(`INSERT INTO locs(name, lat, lng, int) VALUES (${name}, ${latInt}, ${lngInt}, 1)`, function(err) {
                        if (err) { console.error(err.message); return; };

                        processRec(rec + 1);
                    });
                }
            });
        }

        db.get(`SELECT key FROM names WHERE name LIKE '%${query}%'`, function(err, row) {
            if (err) { console.error(err.message); return; };

            if (!!row) {
                processLoc(row.key);
            } else {
                db.run(`INSERT INTO names(name) VALUES ('${query}')`, function(err) {
                    if (err) { console.error(err.message); return; };

                    processLoc(this.lastID);
                });
            }
        });
    };

    processRec(0);
});

app.get('/heatmap', function(req, res) {
    let {name, minLat, maxLat, minLng, maxLng} = req.query;

    let where = '';

    if (!!minLat) {
        minLat = parseFloat(minLat) * PREC;

        if (!!where) {
            where += ' AND ';
        }

        where += `lat >= ${minLat}`;
    }

    if (!!maxLat) {
        maxLat = parseFloat(maxLat) * PREC;

        if (!!where) {
            where += ' AND ';
        }

        where += `lat <= ${maxLat}`;
    }

    if (!!minLng) {
        minLng = parseFloat(minLng) * PREC;

        if (!!where) {
            where += ' AND ';
        }

        where += `lng >= ${minLng}`;
    }

    if (!!maxLng) {
        maxLng = parseFloat(maxLng) * PREC;

        if (!!where) {
            where += ' AND ';
        }

        where += `lng <= ${maxLng}`;
    }

    if (!!name) {
        if (!!where) {
            where += ' AND ';
        }

        where += `name IN (SELECT key FROM names WHERE name LIKE '%${name}%')`;
    }

    if (!!where) {
        where = `WHERE ${where}`;
    }

    const query = `SELECT lat, lng, int FROM locs ${where}`;

    console.log(query);

    db.all(query, function(err, rows) {
        if (err) { console.error(err.message); return; };

        res.json(rows.map(({lat, lng, int}) => {
            return {
                lat: lat / PREC,
                lng: lng / PREC,
                weight: int
            };
        }));
    });
});

app.listen(PORT, () => {
    console.log(`Server started. Listening on port ${PORT}. Database name is ${DB_NAME}.`);
});
