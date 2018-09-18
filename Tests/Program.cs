using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Realtime_Databases;

namespace Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            Dictionary<string, string> tableFormat = new Dictionary<string, string>();
            tableFormat.Add("x", "varchar(5)");
            tableFormat.Add("y", "varchar(5)");
            tableFormat.Add("z", "varchar(260)");

            RealTimeDB db = new RealTimeDB(
                 server: "localhost", DB_TYPE: "mysql", DB_NAME: "TestMonitoringSystem", TABLE_NAME: "TestTableMax11",
                 user_id: "root", user_pw: "123qwe", tableFormat: tableFormat
                 );
            RealTimeDB db1 = new RealTimeDB(
                 server: "localhost", DB_TYPE: "mysql", DB_NAME: "TestMonitoringSystem", TABLE_NAME: "TestTableMax12",
                 user_id: "root", user_pw: "123qwe", tableFormat: tableFormat
                 );
            RealTimeDB db2 = new RealTimeDB(
                 server: "localhost", DB_TYPE: "mysql", DB_NAME: "TestMonitoringSystem", TABLE_NAME: "TestTableMax13",
                 user_id: "root", user_pw: "123qwe", tableFormat: tableFormat
                 );
            db.connect_database();
            db.connect_database_timer();

            db1.connect_database();
            db1.connect_database_timer();

            db2.connect_database();
            db2.connect_database_timer();

            while (true)
            {
                // For Debugging
                Random randomNumber = new Random();
                
                Dictionary<string, string> dic = new Dictionary<string, string>();
                dic.Add("x", randomNumber.Next(0, 150).ToString());
                dic.Add("y", randomNumber.Next(0, 150).ToString());
                dic.Add("z", String.Join("", Enumerable.Repeat<int>(0, 256).ToArray<int>()));

                // Insert your codes
                db.queue.Enqueue(dic);
                db1.queue.Enqueue(dic);
                db2.queue.Enqueue(dic);
                Thread.Sleep(100);
            }

            db.disconnect_database_timer();
            db.disconnect_database();
            
        }

    }
}
