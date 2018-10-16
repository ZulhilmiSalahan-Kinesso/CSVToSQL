using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.FileIO;

namespace CSVtoSQL
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
            DataTable csvData = GetDataTabletFromCSVFile("C:\\Users\\mmd\\Documents\\freelance\\Ajim\\Test - MIQ.CSV");
            InsertDataIntoSQLServerUsingSQLBulkCopy(csvData);
        }

        //read data from file
        public static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable dtTable = new DataTable();
            dtTable.TableName = "sample";
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;

                    var init = false;
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();

                        // Define Data Table Column
                        var index = 0;
                        if (!init)
                        {
                            foreach (var field in fieldData)
                            {
                                DataColumn dtCol = new DataColumn();
                                dtCol.ColumnName = index.ToString();
                                dtTable.Columns.Add(dtCol);
                                index++;
                            }
                            init = true;
                        }

                        // Start push data to Data Table Row
                        index = 0;
                        DataRow dtRow = dtTable.NewRow();
                        foreach (var field in fieldData)
                        {
                            if (field == String.Empty)
                            {
                                dtRow[index.ToString()] = null;
                            }
                            else
                            {
                                dtRow[index.ToString()] = field;
                            }
                            index++;
                        }
                        dtTable.Rows.Add(dtRow);
                    }
                }
            }
            catch (Exception)
            {
            }
            return dtTable;
        }


        protected override void OnStart(string[] args)
        {
        }

        protected override void OnStop()
        {
        }

        //insert Data from File into SQL
        public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvData)
        {
            //set connection setting
            //make sure change this setting
            string connetionString = "Server=tcp:<server name>;Initial Catalog=testing;Persist Security Info=False;User Id=<username>;Password=<password>;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            SqlConnection _connection = new SqlConnection(connetionString);
            try
            {
                _connection.Open();

                // Write data to database
                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(_connection))
                {
                    sqlBulkCopy.DestinationTableName = csvData.TableName;

                    foreach (var column in csvData.Columns)
                        sqlBulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());

                    sqlBulkCopy.WriteToServer(csvData);
                }
            }
            catch (Exception ex)
            {
               Console.WriteLine("Can not open connection ! ");
            }

            
        }
    }
}
