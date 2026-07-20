import assert from "node:assert/strict";
import test from "node:test";
import { LocalCollection } from "./index.js";
import type { Document } from "./index.js";

const tick = () => new Promise<void>((resolve) => queueMicrotask(resolve));

test("live query emits initial and changed snapshots, not irrelevant writes", async () => {
  const collection = new LocalCollection<Document>([{ _id: "a", done: false }]);
  const snapshots: string[][] = [];
  const unsubscribe = collection.find({ done: false }).subscribe((items) => snapshots.push(items.map((item) => item._id)));
  collection.insert({ _id: "b", done: true });
  await tick();
  collection.insert({ _id: "c", done: false });
  await tick();
  unsubscribe();
  collection.insert({ _id: "d", done: false });
  await tick();
  assert.deepEqual(snapshots, [["a"], ["a", "c"]]);
});

test("batch coalesces observer work and snapshots cannot mutate storage", async () => {
  const collection = new LocalCollection<Document>();
  const sizes: number[] = [];
  collection.find().subscribe((items) => {
    sizes.push(items.length);
    if (items[0]) (items[0] as { _id: string })._id = "tampered";
  });
  collection.batch(() => {
    collection.insert({ _id: "a" });
    collection.insert({ _id: "b" });
  });
  await tick();
  assert.deepEqual(sizes, [0, 2]);
  assert.deepEqual(collection.find().fetch().map((item) => item._id), ["a", "b"]);
});

test("local insert/update/delete use the same safe mutation semantics", async () => {
  const collection = new LocalCollection<Document>();
  const inserted = collection.insertOne({ count: 1n, profile: { city: "A" }, tags: ["old", "keep"] });
  assert.match(inserted._id, /^[0-9a-f]{32}$/);
  const snapshots: number[] = [];
  collection.find().subscribe((items) => snapshots.push(items.length));
  const updated = collection.updateOne({ _id: inserted._id }, { $inc: { count: 2n }, $set: { "profile.city": "B" }, $pull: { tags: "old" } });
  await tick();
  assert.deepEqual(updated, { matchedCount: 1, modifiedCount: 1 });
  const found = collection.find({ _id: inserted._id }).fetch()[0] as Document;
  assert.equal(found.count, 3n);
  assert.equal((found.profile as { city: string }).city, "B");
  assert.deepEqual(found.tags, ["keep"]);
  assert.deepEqual(collection.deleteOne({ _id: inserted._id }), { deletedCount: 1 });
  await tick();
  assert.deepEqual(snapshots, [1, 1, 0]);
});

test("findOne uses the shared query compiler and returns an isolated document", () => {
  const collection = new LocalCollection([
    { _id: "00000000000000000000000000000001", rank: 2n, title: "second" },
    { _id: "00000000000000000000000000000002", rank: 1n, title: "first" },
  ]);
  const found = collection.findOne({}, { sort: [{ path: "rank", direction: 1 }] });
  assert.equal(found?.title, "first");
  if (found) (found as { title: string }).title = "mutated";
  assert.equal(collection.findOne({ _id: "00000000000000000000000000000002" })?.title, "first");
});
