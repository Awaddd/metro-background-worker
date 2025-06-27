import { MongoClient } from "mongodb";
import { validateCache } from "./validateCache";

const client = new MongoClient(process.env.MONGODB_URI!);

export default async function () {
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
}
