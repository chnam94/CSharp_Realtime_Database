using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Timers;

namespace Realtime_Databases
{
    public class RealTimeDB
    {
        // MySQL Databases 
        public MySqlConnection conn = null;
        
        public string DB_TYPE = null;
        public string DB_NAME = null;
        public string server = null;
        private string user_id = null;
        private string user_pw = null;
        private string TABLE_NAME = null;
        private int port = 3306;

        Dictionary<string, string> tableFormat = null;

        // DataFormat and Queue define
        public Queue<Dictionary<string, string>> queue;

        // Timer
        private System.Timers.Timer checkQueueTimer;
        
        public RealTimeDB(
            string server = "localhost", 
            string DB_TYPE = "mysql", 
            string DB_NAME = "test",
            string TABLE_NAME = "test",
            string user_id = "root", 
            string user_pw = "root",
            int port = 3306,
            Dictionary<string, string> tableFormat = null
            )
        {
            write_db_data(server, DB_NAME, TABLE_NAME, user_id, user_pw, port, tableFormat);

            switch (DB_TYPE)
            {
                case "mysql":
                    this.DB_TYPE = "MYSQL";
                    this.DB_NAME = DB_NAME;
                    create_databases();
                    create_table(tableFormat);
                    break;
            }

            queue = new Queue<Dictionary<string, string>>();
            
        }

        protected void write_db_data(string server, string db_name, string table_name, string user_id, string user_pw, int port, Dictionary<string, string> tableFormat)
        {
            this.server = server;
            this.DB_NAME = db_name;
            this.TABLE_NAME = table_name;
            this.user_id = user_id;
            this.user_pw = user_pw;
            this.port = port;
            this.tableFormat = tableFormat;
        }
        public bool create_databases()
        {
            bool isCreateDatabases = false;
            string _command = @"Server=" + this.server + ";Uid=" + this.user_id + ";port=" + this.port + ";Pwd=" + this.user_pw + ";";
            string _cmd = "create database if not exists " + this.DB_NAME + ";";

            MySqlConnection conn = new MySqlConnection(_command);
            
            try
            {
                conn.Open();

                MySqlCommand cmd = new MySqlCommand(_cmd, conn);
                cmd.ExecuteNonQuery();
                
                isCreateDatabases = true;
            }
            catch (Exception e)
            {
                
                Console.WriteLine(e.ToString());
                isCreateDatabases = false;
            }
            conn.Close();

            return isCreateDatabases;
        }

        public bool create_table(Dictionary<string, string> tableFormat)
        {
            //Server=localhost;Database=demo_test;UID=root;Password= 
            bool isCreateTable = false;

            string _command = @"Server=" + this.server + ";Database=" + this.DB_NAME + ";Uid=" + this.user_id + ";Pwd=" + this.user_pw + ";";

            //create table if not exist (a,a,a) into (a,a,a);
            string _cmd = "create table if not exists " + this.TABLE_NAME + " (date_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, ";
            string _type = "";
            
            foreach (string key in tableFormat.Keys)
            {
                _type = _type + key + " " + tableFormat[key] + ",";
            }
            //맨 마지막 문자열 "," 지우기위해
            _type = _type.Substring(0, _type.Length - 1);
            
            _cmd = _cmd + _type +  ");";
            Console.WriteLine(_cmd);

            // insert databases command
            MySqlConnection conn = new MySqlConnection(_command);

            try
            {
                conn.Open();
             
                MySqlCommand cmd = new MySqlCommand(_cmd, conn);
                cmd.ExecuteNonQuery();
                
                isCreateTable = true;
                Console.WriteLine("Log | Success create table");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                isCreateTable = false;

                Console.WriteLine("Log | Failed create table");
            }
            conn.Close();

            return isCreateTable;
        }

        public bool connect_database()
        {
            String _command = @"Server=" + this.server + ";Database=" + this.DB_NAME + ";Uid=" + this.user_id + ";Pwd=" + this.user_pw + ";";

            bool isConnected = false;

            try
            {
                conn = new MySqlConnection(_command);
                
                conn.Open();

                isConnected = true;
                Console.WriteLine("Log | Success connect db");
            }
            catch (MySqlException e)
            {
                Console.WriteLine(e.ToString());

                isConnected = false;
                Console.WriteLine("Log | Failed connect db");
            }

            return isConnected;
        }

        public void disconnect_database()
        {
            conn.Close();
        }

        public void connect_database_timer()
        {
            checkQueueTimer = new System.Timers.Timer();
            checkQueueTimer.Interval = 100;
            checkQueueTimer.Elapsed += new ElapsedEventHandler(check_queue);
            checkQueueTimer.Start();
        }

        public void disconnect_database_timer()
        {
            checkQueueTimer.Stop();
        }

        public void check_queue(object sender, ElapsedEventArgs e)
        {
            if (queue.Count != 0)
            {
                checkQueueTimer.Stop();

                while (queue.Count != 0)
                {
                    //Console.WriteLine(" - Log | Insert Data ");
                    this.insert_to_db(queue.Dequeue());
                }
                checkQueueTimer.Start();
            }
        }

        public void insert_to_db(Dictionary<string, string> data)
        {
            /*
             * INSERT INTO 테이블이름(필드이름1, 필드이름2, 필드이름3, ...) VALUES (데이터값1, 데이터값2, 데이터값3, ...)
             */
            switch (DB_TYPE)
            {
                case "MYSQL":
                    string _cmd = "insert into " + this.TABLE_NAME + " ";
                    string _field = "(";
                    string _data = "(";

                    foreach (string key in data.Keys)
                    {
                        _field += key + ",";
                        _data += "\'" + data[key] + "\',";
                    }
                    _field = _field.Substring(0, _field.Length - 1);
                    _data = _data.Substring(0, _data.Length - 1);

                    _field = _field + ")";
                    _data = _data + ");";
                    
                    _cmd = _cmd + _field + " values " + _data;
                    
                    try
                    {
                        MySqlCommand cmd = new MySqlCommand(_cmd, conn);
                        cmd.ExecuteNonQuery();
                        
                        Console.WriteLine("Log | Success insert data");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());

                        Console.WriteLine("Log | Failed insert data");
                    }
                    break;
            }
        }
    }
}
