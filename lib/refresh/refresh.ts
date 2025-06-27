import { MongoClient } from "mongodb";
import { validateCache } from "./validateCache";

console.log(`Mongo db uri (${process.env.MONGODB_URI})`);

const client = new MongoClient(process.env.MONGODB_URI!);

export default async function () {
  console.log("running the refresh script...");
  try {
    await client.connect();
    const db = client.db("metro");

    const { stale, hasData, lastUpdated } = await validateCache(db);
    console.log("stale", stale);
    console.log("hasData", hasData);
    console.log("lastUpdated", lastUpdated);
  } catch (e) {
    console.error("Failed to refresh cache, original error: ", e);
    process.exit(1);
  }
  console.log("script complete");
}
