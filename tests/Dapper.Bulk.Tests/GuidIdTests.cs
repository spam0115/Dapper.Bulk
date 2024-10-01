using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace Dapper.Bulk.Tests;

public class GuidIdTests : SqlServerTestSuite
{
    private List<GuidIdentity> data;
    private List<Guid> ids = new();

    public GuidIdTests()
    {
        data = new List<GuidIdentity>();
        for (var i = 0; i < 10; i++)
        {
            var obj = new GuidIdentity
            {
                Id = Guid.NewGuid(),
                Int_Col = i,
                CreateDate = DateTime.UtcNow,
                Name = i.ToString() + " user field",
            };

            data.Add(obj);

            ids.Add(obj.Id);
        }
    }

    [Fact]
    public void InsertBulk()
    {
        using var connection = GetConnection();
        connection.Open();

        connection.BulkInsert(data, null, 0, 30, true);

        var query = "SELECT * FROM GuidIdentity WHERE Id IN @Ids";
        var inserted = connection.Query<GuidIdentity>(query, new { Ids = ids });

        foreach (var item in inserted)
        {
            var refItem = data.FirstOrDefault(o => o.Id == item.Id);
            IsValidInsert(item, refItem, true);
        }
    }

    [Fact]
    public void InsertBulkAndSelect()
    {
        using var connection = GetConnection();
        connection.Open();

        var inserted = connection.BulkInsertAndSelect(data, null, 0, 30, false).ToList();
        for (var i = 0; i < data.Count; i++)
        {
            IsValidInsert(inserted[i], data[i], false);
        }
    }

    private static void IsValidInsert(GuidIdentity inserted, GuidIdentity toBeInserted, bool checkIdentity = true)
    {
        if (checkIdentity)
            inserted.Id.Should().Be(toBeInserted.Id);
        else
            inserted.Id.Should().NotBe(toBeInserted.Id);

        inserted.Int_Col.Should().Be(toBeInserted.Int_Col);
        inserted.CreateDate.Should().Be(toBeInserted.CreateDate);
        inserted.Name.Should().Be(toBeInserted.Name);
    }
    
    [Table("GuidIdentity")]
    public class GuidIdentity
    {
        [Key]
        public Guid Id { get; set; }

        public int Int_Col { get; set; }

        public DateTime CreateDate { get; set; }

        public string Name { get; set; }
    }
}
