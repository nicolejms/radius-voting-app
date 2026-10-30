using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                var pgsql = OpenDbConnection(BuildDbConnectionString());
                var redisConn = OpenRedisConnection();
                var redis = redisConn.GetDatabase();

                // Keep alive is not implemented in Npgsql yet. This workaround was recommended:
                // https://github.com/npgsql/npgsql/issues/1214#issuecomment-235828359
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { vote = "", voter_id = "" };
                while (true)
                {
                    // Slow down to prevent CPU spike, only query each 100ms
                    Thread.Sleep(100);

                    // Reconnect redis if down
                    if (redisConn == null || !redisConn.IsConnected) {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection();
                        redis = redisConn.GetDatabase();
                    }
                    string json = redis.ListLeftPopAsync("votes").Result;
                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");
                        // Reconnect DB if down
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection(BuildDbConnectionString());
                        }
                        else
                        { // Normal +1 vote requested
                            UpdateVote(pgsql, vote.voter_id, vote.vote);
                        }
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (SocketException)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
                catch (DbException)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            var command = connection.CreateCommand();
            command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                        id VARCHAR(255) NOT NULL UNIQUE,
                                        vote VARCHAR(255) NOT NULL
                                    )";
            command.ExecuteNonQuery();

            return connection;
        }

        private static string GetEnv(string name, string defaultValue)
        {
            var value = Environment.GetEnvironmentVariable(name);
            return string.IsNullOrEmpty(value) ? defaultValue : value;
        }

        private static bool GetEnvFlag(string name)
        {
            var value = GetEnv(name, "false").ToLowerInvariant();
            return value == "1" || value == "true" || value == "yes";
        }

        private static string BuildDbConnectionString()
        {
            var builder = new NpgsqlConnectionStringBuilder
            {
                Host = GetEnv("POSTGRES_HOST", "db"),
                Port = int.Parse(GetEnv("POSTGRES_PORT", "5432")),
                Username = GetEnv("POSTGRES_USER", "postgres"),
                Password = GetEnv("POSTGRES_PASSWORD", "postgres"),
                Database = GetEnv("POSTGRES_DB", "postgres"),
                SslMode = Enum.Parse<SslMode>(GetEnv("POSTGRES_SSLMODE", "Disable"), true)
            };
            return builder.ConnectionString;
        }

        private static ConnectionMultiplexer OpenRedisConnection()
        {
            var hostname = GetEnv("REDIS_HOST", "redis");
            var port = int.Parse(GetEnv("REDIS_PORT", "6379"));
            var useSsl = GetEnvFlag("REDIS_SSL");

            var options = new ConfigurationOptions
            {
                Password = Environment.GetEnvironmentVariable("REDIS_PASSWORD"),
                Ssl = useSsl,
                AbortOnConnectFail = true
            };

            if (useSsl)
            {
                // TLS certificate validation needs the host name, not an IP address
                options.EndPoints.Add(hostname, port);
                options.SslHost = hostname;
            }
            else
            {
                // Use IP address to workaround https://github.com/StackExchange/StackExchange.Redis/issues/410
                var ipAddress = GetIp(hostname);
                Console.WriteLine($"Found redis at {ipAddress}");
                options.EndPoints.Add(ipAddress, port);
            }

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(options);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}