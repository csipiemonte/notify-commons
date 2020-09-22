const express = require('express');
const bodyParser = require('body-parser');
const util = require("util");
const app = express();

app.use(bodyParser.json());
app.use(require('./cache').middleware("mex_cache"));
app.get("/p/:id", async function(req, res) {
  res.set("pippo", "pluto")
  res.status(200).json({ "ok": req.params.id, "date": new Date().toISOString() });
});

app.post("/p/:id", async function(req, res, next) {
  res.status(201).send("OK");
})

app.listen(8080, async function() {
  console.log("server started");
})