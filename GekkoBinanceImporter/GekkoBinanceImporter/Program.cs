using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Binance.Net;
using Binance.Net.Objects;
using System.IO;
using System.Data.SQLite;

namespace GekkoBinanceImporter
{
    class Program
    {
        // Args = [Asset][Currency][start][end][gekko location]
        static void Main(string[] args)
        {
            string asset;
            string currency;
            string start;
            string end;
            string gekkoLocation;

            if (args.Length < 4)
            {
                //TODO: print help
                Console.WriteLine("pleas pass these parameters [asset][currency][start][end][gekko location]");
                return;
            }
            else
            {
                asset = args[0];
                currency = args[1];
                start = args[2];
                end = args[3];
                gekkoLocation = args[4];
            }


            //asset = "ETH";
            //currency = "BTC";
            //start = "2017-12-08";
            //end = "2018-1-10";
            //gekkoLocation = @"C:\gekko";

            var binanceDBLocation = Path.Combine(gekkoLocation, "history","binance_0.1.db");


            var symbol = asset + currency;
            var startTime = DateTime.Parse(start).ToUniversalTime();
            var endTime = DateTime.Parse(end).ToUniversalTime();
            
            Console.WriteLine($"Getting Candles for {asset}|{currency}");
            Console.WriteLine($"From {startTime} to {endTime}");
            Console.WriteLine($"Storing to \"{binanceDBLocation}\" [Exists: {File.Exists(binanceDBLocation)}]");
            

            BinanceDefaults.SetDefaultLogOutput(Console.Out);

            SQLiteConnection dbConnection = null;
            try
            {
                dbConnection = new SQLiteConnection($"Data Source={binanceDBLocation};Version=3;");
                dbConnection.Open();

                Console.Write("Making Sure Table Exists...");
                var ensureTableSQL = $"CREATE TABLE IF NOT EXISTS candles_{asset}_{currency} ( id INTEGER PRIMARY KEY AUTOINCREMENT, start INTEGER UNIQUE, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, vwp REAL NOT NULL, volume REAL NOT NULL, trades INTEGER NOT NULL )";
                var ensureTableCommand = new SQLiteCommand(ensureTableSQL, dbConnection);
                ensureTableCommand.ExecuteNonQuery();
                Console.WriteLine("Does now.");


                using (var client = new BinanceClient())
                using (var socketClient = new BinanceSocketClient())
                {
                    //Do this for every 2 weeks at a time. Binance limit is 500 on klines
                    DateTime segmentStartTime = startTime;
                    DateTime segmentEndTime = startTime.AddDays(14);
                    bool done = false;
                    for (int week = 1; !done; week++)
                    {
                        if (segmentEndTime >= endTime)
                        {
                            segmentEndTime = endTime;
                            done = true;
                        }

                        Console.Write($"Getting candles for period [{segmentStartTime.ToString("yyyy-MM-dd hh:mm")}]-[{segmentEndTime.ToString("yyyy-MM-dd hh:mm")}]...");

                        var klines = client.GetKlines(symbol, KlineInterval.OneHour, startTime: segmentStartTime, endTime: segmentEndTime);

                        if (klines.Success)
                        {
                            Console.Write($"Got {klines.Data.Count()} candles.");
                            // Don't know what vwp is
                            string sql = $"replace into candles_{asset}_{currency} (start, open, high, low, close, vwp, volume, trades) values "
                                + string.Join(",", klines.Data.Select(k => $"(\"{k.OpenTime.ToFileTimeUtc().ToString()}\",\"{k.Open}\",\"{k.High}\",\"{k.Low}\",\"{k.Close}\",\"{k.Open}\",\"{k.Volume}\",\"{k.Trades}\")").ToArray())
                                + ";";

                            Console.Write("Saving to DB...");
                            var command = new SQLiteCommand(sql, dbConnection);
                            command.ExecuteNonQuery();
                            Console.WriteLine("Saved.");
                        }
                        else
                        {
                            Console.WriteLine($"Could not get klines\nCode: {klines.Error.Code}\nMessage: {klines.Error.Message}");
                            return;
                        }

                        // jump forward 2 weeks
                        segmentStartTime = segmentEndTime;
                        segmentEndTime = segmentEndTime.AddDays(14);
                    }
                }
                Console.WriteLine("Finished.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                dbConnection?.Close();
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey(true);
        }

    }
}
