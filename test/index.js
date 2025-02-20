const express = require("express");
const bodyParser = require("body-parser");
const dotenv = require("dotenv");

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(bodyParser.json());

app.listen(PORT, () =>
  console.log(`Server running on port: http://localhost:${PORT}`),
);

app.get("/", (req, res) => {
  const { productId } = req.body;
  // store this productid in db
  res.send("Hello boss");
});
