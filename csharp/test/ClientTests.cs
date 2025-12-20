/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Data.Common;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;
using AdbcClient = Apache.Arrow.Adbc.Client;
using AdbcTests = Apache.Arrow.Adbc.Tests;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Class for testing the ADBC Client using the BigQuery ADBC driver.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ClientTests
    {
        private BigQueryTestConfiguration _testConfiguration;
        readonly List<BigQueryTestEnvironment> _environments;
        readonly ITestOutputHelper _outputHelper;

        public ClientTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanClientExecuteUpdate()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection adbcConnection = GetAdbcConnection(environment))
                {
                    adbcConnection.Open();

                    string[] queries = BigQueryTestingUtils.GetQueries(environment);

                    List<int> expectedResults = new List<int>() { -1, 1, 1 };

                    AdbcTests.ClientTests.CanClientExecuteUpdate(adbcConnection, environment, queries, expectedResults);
                }
            }
        }

        /// <summary>
        /// Validates if the client can get the schema.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanClientGetSchema()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection adbcConnection = GetAdbcConnection(environment))
                {
                    AdbcTests.ClientTests.CanClientGetSchema(adbcConnection, environment);
                }
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanClientExecuteQuery()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection adbcConnection = GetAdbcConnection(environment))
                {
                    AdbcTests.ClientTests.CanClientExecuteQuery(adbcConnection, environment, environmentName: environment.Name);
                }
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [SkippableFact, Order(4)]
        public void VerifyTypesAndValues()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection dbConnection = GetAdbcConnection(environment))
                {
                    SampleDataBuilder sampleDataBuilder = BigQueryData.GetSampleData();

                    AdbcTests.ClientTests.VerifyTypesAndValues(dbConnection, sampleDataBuilder, environment.Name);
                }
            }
        }

        [SkippableFact]
        public void VerifySchemaTablesWithNoConstraints()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection adbcConnection = GetAdbcConnection(environment, includeTableConstraints: false))
                {
                    adbcConnection.Open();

                    string schema = "Tables";

                    var tables = adbcConnection.GetSchema(schema);

                    Assert.True(tables.Rows.Count > 0, $"No tables were found in the schema '{schema}' for environment '{environment.Name}'");
                }
            }
        }


        [SkippableFact]
        public void VerifySchemaTables()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection adbcConnection = GetAdbcConnection(environment))
                {
                    adbcConnection.Open();

                    var collections = adbcConnection.GetSchema("MetaDataCollections");
                    Assert.Equal(7, collections.Rows.Count);
                    Assert.Equal(2, collections.Columns.Count);

                    var restrictions = adbcConnection.GetSchema("Restrictions");
                    Assert.Equal(11, restrictions.Rows.Count);
                    Assert.Equal(3, restrictions.Columns.Count);

                    var catalogs = adbcConnection.GetSchema("Catalogs");
                    Assert.Single(catalogs.Columns);
                    var catalog = (string?)catalogs.Rows[0].ItemArray[0];

                    catalogs = adbcConnection.GetSchema("Catalogs", new[] { catalog });
                    Assert.Equal(1, catalogs.Rows.Count);

                    string random = "X" + Guid.NewGuid().ToString("N");

                    catalogs = adbcConnection.GetSchema("Catalogs", new[] { random });
                    Assert.Equal(0, catalogs.Rows.Count);

                    var schemas = adbcConnection.GetSchema("Schemas", new[] { catalog });
                    Assert.Equal(2, schemas.Columns.Count);
                    var schema = (string?)schemas.Rows[0].ItemArray[1];

                    schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, schema });
                    Assert.Equal(1, schemas.Rows.Count);

                    schemas = adbcConnection.GetSchema("Schemas", new[] { random });
                    Assert.Equal(0, schemas.Rows.Count);

                    schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, random });
                    Assert.Equal(0, schemas.Rows.Count);

                    schemas = adbcConnection.GetSchema("Schemas", new[] { random, random });
                    Assert.Equal(0, schemas.Rows.Count);

                    var tableTypes = adbcConnection.GetSchema("TableTypes");
                    Assert.Single(tableTypes.Columns);

                    var tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema });
                    Assert.Equal(4, tables.Columns.Count);

                    tables = adbcConnection.GetSchema("Tables", new[] { catalog, random });
                    Assert.Equal(0, tables.Rows.Count);

                    tables = adbcConnection.GetSchema("Tables", new[] { random, schema });
                    Assert.Equal(0, tables.Rows.Count);

                    tables = adbcConnection.GetSchema("Tables", new[] { random, random });
                    Assert.Equal(0, tables.Rows.Count);

                    tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema, random });
                    Assert.Equal(0, tables.Rows.Count);

                    var columns = adbcConnection.GetSchema("Columns", new[] { catalog, schema });
                    Assert.Equal(16, columns.Columns.Count);
                }
            }
        }

        /// <summary>
        /// Validates that the client can perform paged queries using SQL LIMIT and OFFSET
        /// without knowing the total result count in advance.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanClientExecutePagedQuery()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using (AdbcClient.AdbcConnection adbcConnection = GetAdbcConnection(environment))
                {
                    adbcConnection.Open();
                    int records = 5000;

                    // Create a test query - pretend we don't know how many results there are
                    string baseQuery = $"SELECT number FROM UNNEST(GENERATE_ARRAY(1, {records})) AS number ORDER BY number";
                    int pageSize = 100;

                    List<int> allResults = new List<int>();
                    int pageCount = 0;
                    int offset = 0;
                    bool hasMorePages = true;

                    _outputHelper.WriteLine($"Testing SQL-based paged query for environment: {environment.Name}");
                    _outputHelper.WriteLine($"Page size: {pageSize}");

                    // Keep fetching pages until we get fewer rows than the page size
                    while (hasMorePages)
                    {
                        pageCount++;
                        _outputHelper.WriteLine($"Fetching page {pageCount} (offset {offset})...");

                        using (AdbcClient.AdbcCommand command = adbcConnection.CreateCommand())
                        {
                            // Add LIMIT and OFFSET to the query for pagination
                            command.CommandText = $"{baseQuery} LIMIT {pageSize + 1} OFFSET {offset}";

                            using (var reader = command.ExecuteReader())
                            {
                                int rowsInPage = 0;
                                while (reader.Read())
                                {
                                    int number = reader.GetInt32(0);
                                    allResults.Add(number);
                                    rowsInPage++;
                                }

                                _outputHelper.WriteLine($"Page {pageCount} retrieved {rowsInPage} rows");

                                if (rowsInPage > pageSize)
                                {
                                    // There are more results - only use first pageSize rows
                                    allResults.RemoveAt(allResults.Count - 1); // Remove the extra row
                                    hasMorePages = true;
                                }
                                else
                                {
                                    hasMorePages = false;
                                }

                                if (hasMorePages)
                                {
                                    // Move to the next page
                                    offset += pageSize;
                                }
                            }
                        }
                    }

                    _outputHelper.WriteLine($"Total pages retrieved: {pageCount}");
                    _outputHelper.WriteLine($"Total rows retrieved: {allResults.Count}");

                    // For this test, we know there should be 5000 results
                    // But in real scenarios, you wouldn't know this upfront
                    Assert.Equal(records, allResults.Count);

                    // Verify the results are in the correct order
                    for (int i = 0; i < allResults.Count; i++)
                    {
                        Assert.Equal(i + 1, allResults[i]);
                    }

                    _outputHelper.WriteLine($"SQL-based paging test completed successfully for environment: {environment.Name}");
                }
            }
        }

        private AdbcClient.AdbcConnection GetAdbcConnection(
            BigQueryTestEnvironment environment,
            bool includeTableConstraints = true
        )
        {
            environment.IncludeTableConstraints = includeTableConstraints;

            if (string.IsNullOrEmpty(environment.StructBehavior))
            {
                Dictionary<string, string> connectionParameters = BigQueryTestingUtils.GetBigQueryParameters(environment);

                return new AdbcClient.AdbcConnection(
                    new BigQueryDriver(),
                    connectionParameters,
                    new Dictionary<string, string>()
                );
            }
            else
            {
                return GetAdbcConnectionUsingConnectionString(environment, includeTableConstraints);
            }
        }

        private AdbcClient.AdbcConnection GetAdbcConnectionUsingConnectionString(
            BigQueryTestEnvironment environment,
            bool includeTableConstraints = true
        )
        {
            Dictionary<string, string> connectionParameters = BigQueryTestingUtils.GetBigQueryParameters(environment);

            if (!string.IsNullOrEmpty(environment.StructBehavior))
                connectionParameters.Add("StructBehavior", environment.StructBehavior!);

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);

            foreach (string key in connectionParameters.Keys)
            {
                builder[key] = connectionParameters[key];
            }

            return new AdbcClient.AdbcConnection(builder.ConnectionString)
            {
                AdbcDriver = new BigQueryDriver()
            };
        }
    }
}
