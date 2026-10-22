using System.Globalization;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Site;

/** Form messages and daily page-view counters in MongoDB (DATABASE_URL). Nothing about visitors is kept. */
public sealed class MongoStore : IStore
{
    private readonly IMongoCollection<BsonDocument> _messages;
    private readonly IMongoCollection<BsonDocument> _counts;
    private readonly IMongoCollection<BsonDocument> _counters;
    private readonly IMongoCollection<BsonDocument> _hits;
    private readonly int _keepMessages;

    public MongoStore(string url, int keepMessages)
    {
        var address = MongoUrl.Create(url);
        var db = new MongoClient(address).GetDatabase(address.DatabaseName ?? "site");
        _messages = db.GetCollection<BsonDocument>("form_messages");
        _counts = db.GetCollection<BsonDocument>("daily_counts");
        _counters = db.GetCollection<BsonDocument>("counters");
        _hits = db.GetCollection<BsonDocument>("rate_hits");
        _keepMessages = keepMessages;
        _counts.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Ascending("day").Ascending("kind").Ascending("key"), new CreateIndexOptions { Unique = true }));
        // Hits older than a minute go by themselves (counting looks at the last minute only).
        _hits.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Ascending("at"), new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(60) }));
        _hits.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Ascending("bucket").Ascending("at")));
    }

    private static string Today(int offset = 0) => DateTime.UtcNow.Date.AddDays(offset).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static FilterDefinitionBuilder<BsonDocument> F => Builders<BsonDocument>.Filter;

    public void AddMessage(Message m)
    {
        var next = _counters.FindOneAndUpdate(F.Eq("_id", "form_messages"), Builders<BsonDocument>.Update.Inc("seq", 1L), new FindOneAndUpdateOptions<BsonDocument> { IsUpsert = true, ReturnDocument = ReturnDocument.After });
        var id = next["seq"].ToInt64();
        var fields = new BsonArray(m.Fields.Select(f => new BsonDocument { ["label"] = f.Label, ["value"] = f.Value }));
        var created = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
        _messages.InsertOne(new BsonDocument { ["_id"] = id, ["formId"] = m.FormId, ["page"] = m.Page, ["fields"] = fields, ["createdAt"] = created });
        // Only the newest are kept.
        var oldest = _messages.Find(F.Empty).Sort(Builders<BsonDocument>.Sort.Descending("_id")).Skip(_keepMessages).Limit(1).FirstOrDefault();
        if (oldest != null) _messages.DeleteMany(F.Lte("_id", oldest["_id"]));
    }

    public List<object> Messages(int limit) =>
        _messages.Find(F.Empty).Sort(Builders<BsonDocument>.Sort.Descending("_id")).Limit(limit).ToList()
            .Select(d => (object)new
            {
                id = d["_id"].ToInt64(),
                formId = d["formId"].AsString,
                page = d["page"].AsString,
                fields = d["fields"].AsBsonArray.Select(f => new { label = f["label"].AsString, value = f["value"].AsString }).ToList(),
                createdAt = d["createdAt"].AsString,
            })
            .ToList();

    public bool DeleteMessage(long id) => _messages.DeleteOne(F.Eq("_id", id)).DeletedCount > 0;

    public void CountView(string path, string? referrer)
    {
        var upsert = new UpdateOptions { IsUpsert = true };
        _counts.UpdateOne(F.Eq("day", Today()) & F.Eq("kind", "view") & F.Eq("key", path), Builders<BsonDocument>.Update.Inc("count", 1L), upsert);
        if (referrer != null) _counts.UpdateOne(F.Eq("day", Today()) & F.Eq("kind", "referrer") & F.Eq("key", referrer), Builders<BsonDocument>.Update.Inc("count", 1L), upsert);
    }

    private List<object> Totals(string kind, string since) =>
        _counts.Aggregate()
            .Match(F.Eq("kind", kind) & F.Gte("day", since))
            .Group(new BsonDocument { ["_id"] = "$key", ["count"] = new BsonDocument("$sum", "$count") })
            .Sort(new BsonDocument { ["count"] = -1, ["_id"] = 1 })
            .ToList()
            .Select(d => (object)new { key = d["_id"].AsString, count = d["count"].ToInt64() })
            .ToList();

    public object Stats(int days)
    {
        var since = Today(-(days - 1));
        var daily = _counts.Aggregate()
            .Match(F.Eq("kind", "view") & F.Gte("day", since))
            .Group(new BsonDocument { ["_id"] = "$day", ["views"] = new BsonDocument("$sum", "$count") })
            .Sort(new BsonDocument("_id", 1))
            .ToList()
            .Select(d => (object)new { day = d["_id"].AsString, views = d["views"].ToInt64() })
            .ToList();
        return new { days, daily, pages = Totals("view", since), referrers = Totals("referrer", since) };
    }

    public bool? Allow(string bucket, int perMinute)
    {
        var since = DateTime.UtcNow.AddSeconds(-60);
        if (_hits.CountDocuments(F.Eq("bucket", bucket) & F.Gt("at", since)) >= perMinute) return false;
        _hits.InsertOne(new BsonDocument { ["bucket"] = bucket, ["at"] = DateTime.UtcNow });
        return true;
    }
}
