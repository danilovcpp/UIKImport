using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Xml;

namespace UIKImport
{
	static class Extention
	{
		public static void ForeachAndForeachExceptLast<T>(this IEnumerable<T> source, Action<T> forEach, Action<T> forEachExceptLast = null)
		{
			using (var enumerator = source.GetEnumerator())
			{
				if (enumerator.MoveNext())
				{
					var item = enumerator.Current; ;

					while (enumerator.MoveNext())
					{
						forEach(item);
						if (forEachExceptLast != null)
							forEachExceptLast(item);
						item = enumerator.Current;
					}
					forEach(item);
				}
			}
		}
	}

	class Program
	{
		public static string GenerateInsertStatement(string table, Dictionary<string, object> data)
		{
			StringBuilder statement = new StringBuilder();

			statement.Append($"INSERT INTO public.\"{table}\" (");

			data.ForeachAndForeachExceptLast(column =>
			{
				statement.Append($"\"{column.Key}\"");
			},
				column =>
				{
					statement.Append(", ");
				});

			statement.Append($") VALUES(");

			data.ForeachAndForeachExceptLast(column =>
			{
				statement.Append($"\'{column.Value}\'");
			},
				column =>
				{
					statement.Append(", ");
				});

			statement.Append($")");

			statement.Append($" RETURNING \"RecID\"");

			return statement.ToString();
		}


		static void Main(string[] args)
		{
			//XmlReader reader = XmlReader.Create(@"Adriesa_UIK_version_3.xml");
			NpgsqlConnection conn = new NpgsqlConnection("server=localhost;port=5432;user id=postgres;password=postgres;database=uik");
			conn.Open();

			IDbCommand command = conn.CreateCommand();

			XmlDocument doc = new XmlDocument();
			doc.Load(@"Adriesa_UIK_version_3.xml");

			var records = doc.GetElementsByTagName("nsi:record");

			var dict = new Dictionary<string, object>();
			var columns = new List<string>
			{
				"SubjectCode",
				"SubjectName",
				"UIKNumber",
				"CountryCode",
				"IntercityCode",
				"Phone",
				"Latitude",
				"Longitude",
				"Address",
				"UIKCode",
				"UIKPhone",
				"UIKLatitude",
				"UIKLongtitude",
				"UIKAddress"
			};
			var count = 0;

			for (int i = 0; i< records.Count; i++)
			{
				var record = records.Item(i);
				var uid = record.Attributes.Item(0).Value;

				var attrs = record.ChildNodes;

				for(int j = 0; j< attrs.Count; j++)
				{
					var attr = attrs.Item(j);
					var val = attr.InnerText;

					dict.Add(columns[j], val);
				}

				dict.Add("RecID", uid);
				dict.Add("State", 1);
				var sql = GenerateInsertStatement("UIK", dict);

				command.CommandText = sql;
				var rows = command.ExecuteNonQuery();

				count += rows;
				Console.WriteLine($"ROWS: {count.ToString()}");

				dict.Clear();
			}
			
			/*
			
			var columnNumber = 0;
			while (reader.Read())
			{
				var name = reader.Name;
				//reader.Read();

				if (name == "nsi:record")
				{
					reader.Read();
					for (int i = 0; i < 14; i++)
					{
						reader.Read();
						reader.Read();
						reader.Read();

						var name1 = reader.Name;
						reader.Read();
						var value1 = reader.Value;

						dict.Add(columns[columnNumber++], value1);

						reader.Read();
						reader.Read();
						reader.Read();
						reader.Read();
					}

					var sql = GenerateInsertStatement("UIK", dict);

					command.CommandText = sql;
					var rows = command.ExecuteNonQuery();

					count += rows;
					Console.WriteLine($"ROWS: {count.ToString()}");
					
					dict.Clear();
					columnNumber = 0;

					reader.Read();
				}
			}*/

			conn.Close();
			Console.WriteLine("END");
			Console.ReadLine();
		}
	}
}
