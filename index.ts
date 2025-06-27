import { MongoClient } from "mongodb";
import { Db } from "mongodb";
import dotenv from "dotenv";
import { DATA_COLLECTION, META_COLLECTION } from "./constants";
import { StopSearchData, StopSearchResponse } from "./types/stop-search";
import { StatisticDocument } from "./types/stats";
dotenv.config();

const client = new MongoClient(process.env.MONGODB_URI!);

(async () => {
  client.connect();
  console.log("worker is now online");
  console.log(`Mongo db uri (${process.env.MONGODB_URI})`);
  await refresh();
  await client.close();
})();

async function refresh() {
  console.log("running the refresh script...");
  try {
    await client.connect();
    const db = client.db("metro");

    const { stale, hasData, lastUpdated } = await validateCache(db);
    console.log("stale", stale);
    console.log("hasData", hasData);
    console.log("lastUpdated", lastUpdated);

    if (stale) {
      await fetchAndPersist(db);
    }
  } catch (e) {
    console.error("Failed to update cache, original error: ", e);
    process.exit(1);
  }
  console.log("script complete");
}

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

async function validateCache(db: Db): Promise<CacheStatus> {
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

async function fetchAndPersist(db: Db) {
  console.time("overallFetchAndPersistComputedStatistics");
  const freshData = await fetchStopSearchData();
  const transformed = freshData.map(transformData);
  const statistics = calculateStatistics(transformed);
  await persist(db, statistics);
  console.timeEnd("overallFetchAndPersistComputedStatistics");
}

// when transforming, we will ignore many unnecessary properties, drastically reducing the size of objects
// we store in mongo, this helps both with performance and keeping costs low
// TODO: remove unnecessary properties not used in calculations like legislation
function transformData(data: StopSearchResponse): StopSearchData {
  return {
    ageRange: data.age_range as StopSearchData["ageRange"],
    // officerDefinedEthnicity: data.officer_defined_ethnicity,
    // involvedPerson: data.involved_person,
    // selfDefinedEthnicity: data.self_defined_ethnicity,
    gender: data.gender,
    // legislation: data.legislation,
    // outcomeLinkedToObjectOfSearch: data.outcome_linked_to_object_of_search,
    datetime: data.datetime,
    outcome: data.outcome,
    // outcomeObject: data.outcome_object,
    // location: data.location,
    objectOfSearch: data.object_of_search,
    // operation: data.operation,
    // operationName: data.operation_name,
    type: data.type as StopSearchData["type"],
    // removalOfMoreThanOuterClothing: data.removal_of_more_than_outer_clothing,
  };
}

function calculateStatistics(data: StopSearchData[]) {
  console.time("calculateStatistics");

  // create sets to collect all possible filters
  const uniqueMonths = new Set<string>();
  const uniqueAgeGroups = new Set<StopSearchData["ageRange"]>();
  const uniqueTypes = new Set<StopSearchData["type"]>();

  for (const item of data) {
    uniqueMonths.add(item.datetime.slice(0, 7));
    uniqueAgeGroups.add(item.ageRange);
    uniqueTypes.add(item.type);
  }

  const statistics: StatisticDocument[] = [];

  // precompute statistics for each filter combination
  for (const month of uniqueMonths) {
    for (const ageGroup of uniqueAgeGroups) {
      for (const type of uniqueTypes) {
        // try to find an item in testData where the month, ageGroup and type are the same

        const matchedItems = [];

        // when determining uniqye days, slice date instead of formatting with date-fns for improved performance
        // creating a date obj and formatting each would add unnecessary computation
        // especially since we are dealing with many records
        const uniqueDays = new Set();
        const uniqueGenders = new Set();
        const uniqueObjects = new Set();
        const uniqueOutcomes = new Set();

        let arrestCount = 0;
        const ages = new Map<string, number>();
        const genders = new Map<string, number>();
        const objectsOfSearch = new Map<string, number>();
        const outcomes = new Map<string, number>();

        // do all calculations in one loop as we are going through a huge number of records
        // so inefficient to calculate separately
        for (const item of data) {
          if (
            !(
              item.datetime.includes(month) &&
              item.ageRange === ageGroup &&
              item.type === type
            )
          ) {
            continue;
          }

          matchedItems.push(item);

          uniqueDays.add(item.datetime.slice(0, 10));
          uniqueGenders.add(item.gender);
          uniqueObjects.add(item.objectOfSearch);
          uniqueOutcomes.add(item.outcome);

          if (item.outcome?.toLowerCase() === "arrest") {
            arrestCount += 1;
          }

          const key = item.ageRange == null ? "null" : item.ageRange;
          ages.set(key, (ages.get(key) ?? 0) + 1);

          const genderKey = item.gender == null ? "null" : item.gender;
          genders.set(genderKey, (genders.get(genderKey) ?? 0) + 1);

          // this is getting repetitive, clean up this file and make reusable function
          const objectKey =
            item.objectOfSearch == null ? "null" : item.objectOfSearch;

          objectsOfSearch.set(
            objectKey,
            (objectsOfSearch.get(objectKey) ?? 0) + 1
          );

          const outcomeKey = item.outcome == null ? "null" : item.outcome;
          outcomes.set(outcomeKey, (genders.get(outcomeKey) ?? 0) + 1);
        }

        const totalSearches = matchedItems.length;

        if (matchedItems.length === 0) {
          continue;
        }

        const statistic: StatisticDocument = {
          month: month,
          ageRange: ageGroup,
          type: type,
          totalSearches: totalSearches,
          arrests: arrestCount,
          daysWithData: uniqueDays.size,
          genders: genders,
          objectsOfSearch: objectsOfSearch,
          outcomes: outcomes,
        };

        statistics.push(statistic);
      }
    }
  }

  console.timeEnd("calculateStatistics");
  return statistics;
}

async function persist(db: Db, docs: StatisticDocument[]) {
  const dataCollection = db.collection<StatisticDocument>(DATA_COLLECTION);
  const meta = db.collection<MetaDocument>(META_COLLECTION);

  let updated = false;

  try {
    const result = await dataCollection.deleteMany({});
    console.log(`Deleted ${result.deletedCount} record(s)`);
  } catch (e) {
    console.error("Failed to delete expired cache data, original error:", e);
    return updated;
  }

  try {
    const result = await dataCollection.insertMany(docs);
    console.log(`Inserted ${result.insertedCount} record(s)`);

    updated = result.insertedCount === docs.length;
  } catch (e) {
    console.error(
      "Failed to update mongo cache with fresh data, original error:",
      e
    );
  }

  if (!updated) {
    // should be setting hasData to false if the insert above failed
    return false;
  }

  // update last updated if successfully stored fresh data
  try {
    const id = "stop-and-search-cache";
    const doc = {
      _id: id,
      lastUpdated: new Date(),
      hasData: true,
    };

    const result = await meta.updateOne(
      { _id: id },
      { $set: doc },
      { upsert: true }
    );

    console.log("updated meta with", result.upsertedId);
    return true;
  } catch (e) {
    console.error("Failed to update mongo meta, original error", e);
    return false;
  }
}

type AvailableDatesResponse = {
  date: string;
  "stop-and-search": string[];
};

async function getAvailableDates() {
  const availableDates = await fetchAvailableDates();
  return filterData(availableDates);
}

async function fetchAvailableDates(): Promise<AvailableDatesResponse[]> {
  const uri = `${process.env.POLICE_API}/crimes-street-dates`;

  try {
    const response = await fetch(uri);
    return (await response.json()) as AvailableDatesResponse[];
  } catch (e) {
    console.error(
      `Failed to fetch available dates at uri ${uri}, original error: `,
      e
    );
  }
  return [];
}

function filterData(availableDates: AvailableDatesResponse[]) {
  const dates: string[] = [];

  availableDates.forEach((object) => {
    if (!("stop-and-search" in object)) {
      return;
    }

    if (object["stop-and-search"].includes("metropolitan")) {
      dates.push(object.date);
    }
  });

  console.log("getting dates for metro...", dates);

  return dates;
}

async function fetchStopSearchData(): Promise<StopSearchResponse[]> {
  console.log("*** fetching fresh data ***");
  const availableDates = await getAvailableDates();

  // temp limit all data to just 7 months
  console.time("fetchRecordsFromApi");
  const data = await batchFetchData(availableDates.slice(0, 7), 10);
  console.timeEnd("fetchRecordsFromApi");
  return data;
}

async function batchFetchData(dates: string[], size: number) {
  console.log("batching array in chunks of", size);

  const results: StopSearchResponse[] = [];

  for (let i = 0; i < dates.length; i += size) {
    console.log("executing batch ", i);
    const chunk = dates.slice(i, i + size);
    const promises = chunk.map((date) => fetchData(date));
    const batchResult = await Promise.all(promises);
    results.push(...batchResult.flat());
  }

  return results;
}

// fetch data from police api
async function fetchData(date: string): Promise<StopSearchResponse[]> {
  const uri = `${process.env.POLICE_API}/stops-force?force=metropolitan&date=${date}`;
  console.log("fetching at uri", uri);

  try {
    const response = await fetch(uri);
    const data = (await response.json()) as StopSearchResponse[];
    return data;
  } catch (e) {
    console.error(`Failed to fetch data at uri ${uri}, original error: `, e);
  }
  return [];
}
