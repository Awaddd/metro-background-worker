import refresh from "./lib/refresh/refresh";
import dotenv from "dotenv";
dotenv.config();

console.log("worker is now online");
refresh();
