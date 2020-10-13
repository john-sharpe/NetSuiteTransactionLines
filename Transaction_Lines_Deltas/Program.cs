using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetSuite.SuiteAnalyticsConnect;
using System.Data.SqlClient;
using System.Configuration;

namespace Transaction_Lines_Deltas
{
    class Program
    {
        static void Main(string[] args)
        {

            SqlConnection targetconnection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["SQLSWConn"].ConnectionString);
            targetconnection.Open();

            OpenAccessCommand command = null;
            OpenAccessCommand commandCnt = null;
            OpenAccessDataReader reader = null;
            OpenAccessConnection connection = new OpenAccessConnection("Host=odbcserver.na2.netsuite.com;Port=1708;ServerDataSource=NetSuite.com;User Id=netsuiteapi2@swisher.com;Password=Swisher2018;CustomProperties='AccountID=4530006;RoleID=3';EncryptionMethod=SSL");
            connection.Open();
            Console.WriteLine("Transcation_Lines");

            //string sLastRunTime = System.Configuration.ConfigurationManager.AppSettings[0].ToString();
            string sLastRunTime = "2019-05-02 16:57:37";

            //command = new OpenAccessCommand("select * from transaction_lines where date_last_modified_gmt < {d '2019-03-01'} and date_last_modified_gmt > {d '2019-01-07'}", connection);
            //command = new OpenAccessCommand("select * from transaction_lines where date_last_modified_gmt > {d '4/18/2019 3:13:38 AM'}", connection);
            commandCnt = new OpenAccessCommand("select count(*) from transaction_lines where date_last_modified_gmt > " + "{ts '" + sLastRunTime + "'}", connection);
            reader = commandCnt.ExecuteReader();
            reader.Read();
            string sCount = reader[0].ToString();

            command = new OpenAccessCommand("select * from transaction_lines where date_last_modified_gmt > " + "{ts '" + sLastRunTime + "'}", connection);
            reader = command.ExecuteReader();
            
            string strInsertQuery = "";
            int insCount = 0;
            int dupCount = 0;

            while (reader.Read())
            {

                    strInsertQuery = "insert into transaction_lines values (";
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string data = reader[i].ToString();

                        if (!reader.IsDBNull(i))
                        {
                            //Console.Write(data + "\t");
                            strInsertQuery += "'" + data.Replace("'", "") + "',";
                        }
                        else
                        {
                            //Console.Write("NULL" + "\t");
                            strInsertQuery += "null,";
                        }
                    }

                    strInsertQuery = strInsertQuery.TrimEnd(',');
                    strInsertQuery += ")";
                    //Console.WriteLine(strInsertQuery);
                    SqlCommand cmdIns = targetconnection.CreateCommand();
                    cmdIns.CommandType = System.Data.CommandType.Text;
                    cmdIns.CommandText = strInsertQuery;
                    try
                    {
                        cmdIns.ExecuteNonQuery();
                        insCount++;
                    }
                    catch (Exception e)
                    {
                        if (e.Message.Contains("Cannot insert duplicate key"))
                        {
                            dupCount++;
                            SqlCommand cmdDel = targetconnection.CreateCommand();
                            cmdDel.CommandType = System.Data.CommandType.Text;
                            cmdDel.CommandText = "delete from transaction_lines where transaction_id = " + reader[188] + " and transaction_line_id = " + reader[189];
                            cmdDel.ExecuteNonQuery();
                            cmdDel.Dispose();
                            cmdIns.ExecuteNonQuery();
                            Console.Write("Inserts: " + insCount + " - Updates: " + dupCount + " of " + sCount + "\r");
                            continue;
                        }
                    }

                Console.Write("Inserts: " + insCount + " - Updates: " + dupCount + " of " + sCount + "\r");

            }

            Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            config.AppSettings.Settings.Remove("LastRunTime");
            config.AppSettings.Settings.Add("LastRunTime", DateTime.UtcNow.ToString("yyyy'-'MM'-'dd' 'HH':'mm':'ss"));
            config.Save(ConfigurationSaveMode.Modified);

        }

    }

}
