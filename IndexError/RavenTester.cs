using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace IndexError
{
    [TestFixture]
    public class RavenTester
    {
        private IDocumentStore _store;

        [SetUp]
        public void SetupDocumentStore()
        {
            _store = new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "IndexTester"};
            _store.Initialize();
            IndexCreation.CreateIndexes(typeof(PersonCountReport).Assembly, _store);
        }

        [Test]
        public void IndexesShouldIncrease()
        {
            var personcount = 0;
            for (var i = 0; i < 1500; i++)
            {
                using(var session = _store.OpenSession())
                {
                    session.Store(new Person { Created = DateTime.Now });
                    session.SaveChanges();
                }
                Thread.Sleep(5);
                using (var session = _store.OpenSession())
                {
                    var result = session.Query<PersonCount>(typeof (PersonCountReport).Name).OrderByDescending(c => c.Date).FirstOrDefault();
                    if (result == null) continue;
                    Assert.That(result.Count, Is.GreaterThanOrEqualTo(personcount));
                    Trace.WriteLine(result.Count);
                    personcount = result.Count;
                }
            }

            using (var session = _store.OpenSession())
            {
                var personsToday = session.Query<PersonCount>(typeof(PersonCountReport).Name).Customize(x => x.WaitForNonStaleResultsAsOfNow()).OrderByDescending(c => c.Date).First().Count;
                var persons = session.Query<Person>().Count();
                Assert.That(personsToday, Is.EqualTo(persons));
            }
        }
    }

    public class PersonCountReport : AbstractIndexCreationTask<Person, PersonCount>
    {
        public PersonCountReport()
        {
            Map = persons => from person in persons
                                     select new PersonCount { Date = person.Created.Date, Count = 1 };
            Reduce = results => from result in results
                                group result by result.Date
                                    into g
                                    select new PersonCount { Date = g.Key, Count = g.Sum(x => x.Count) };
        }
    }

    public class PersonCount {
        public DateTime Date { get; set; }
        public int Count { get; set; }
    }

    public class Person {
        public string Id { get; set; }
        public DateTime Created { get; set; }
    }
}
