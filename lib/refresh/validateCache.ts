import { Db } from "mongodb";
import { META_COLLECTION } from "../../constants";

type MetaDocument = {
  _id: string;
  lastUpdated: Date | null;
  hasData: boolean;
};

type CacheStatus = {
  stale: boolean;
  hasData: boolean;
  lastUpdated: Date | null;
};

export async function validateCache(db: Db): Promise<CacheStatus> {
  const meta = db.collection(META_COLLECTION);

  const lastWeek = new Date();

  lastWeek.setDate(new Date().getDate() - 7);
  lastWeek.setHours(0, 0, 0, 0);

  try {
    const record = await meta.findOne<MetaDocument>({
      lastUpdated: {
        $gte: lastWeek,
      },
    });

    if (record) {
      return {
        stale: false,
        hasData: record.hasData,
        lastUpdated: record.lastUpdated,
      };
    }

    // if we failed to find a record in the last week, check if we have any data at all even if stale
    const record2 = await meta.findOne<MetaDocument>({});

    if (!record2) {
      throw new Error("Meta document does not exist");
    }

    return {
      stale: true,
      hasData: !!record2.hasData,
      lastUpdated: record2.lastUpdated ?? null,
    };
  } catch (e) {
    console.error("Error checking last updated, original error: ", e);
    return {
      stale: true,
      hasData: false,
      lastUpdated: null,
    };
  }
}
