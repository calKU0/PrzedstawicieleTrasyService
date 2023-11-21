using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using System.Threading;
using System.Timers;

namespace GaskaPrzedstawicieleTrasyService
{
    public partial class Service1 : ServiceBase
    {
        DataTable dtWizyty = new DataTable();
        System.Timers.Timer timer = new System.Timers.Timer();
        string sftpHost = ConfigurationManager.AppSettings["SftpHostname"];
        int sftpPort = int.Parse(ConfigurationManager.AppSettings["SftpPort"]);
        string sftpUsername = ConfigurationManager.AppSettings["SftpUsername"];
        string sftpPassword = ConfigurationManager.AppSettings["SftpPassword"];
        string sciezkaZapisu = ConfigurationManager.AppSettings["Ścieżka Zapisu"];
        string godzinaWysylki = ConfigurationManager.AppSettings["Godzina wysylki"];
        string sftpFolderPath = ConfigurationManager.AppSettings["SftpFolderPath"];
        int odpytujCoMinut = int.Parse(ConfigurationManager.AppSettings["Co ile minut odpytywac"]);
        string czyJuzWykonano = "";
        private static AutoResetEvent autoResetEvent = new AutoResetEvent(false);
        SqlConnection connection;

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            ZapiszLog("Uruchomienie usługi");
            try
            {
                Thread Wysylka = new Thread(Watek);
                Wysylka.Start();

            }
            catch (Exception ex)
            {
                ZapiszLog("Błąd OnStart. " + ex.ToString());
                Stop();
            }
        }

        protected override void OnStop()
        {
            ZapiszLog("Zatrzymanie usługi");
            if (connection != null && connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
            timer.Stop();
        }

        public void OnTimer(object sender, ElapsedEventArgs args)
        {
            string aktualnaGodzina = DateTime.Now.Hour.ToString();
            string data = DateTime.Now.ToShortDateString();

            if (aktualnaGodzina == godzinaWysylki && czyJuzWykonano != data) // jesli juz wykonalem operacje o zadanej godzinie w dzisiejszym dniu to juz jej nie wykonam ponownie
            {
                Thread threadWysylka = new Thread(WysylkaWizyt);
                threadWysylka.Start();
                czyJuzWykonano = data;
            }
        }

        private void Watek()
        {
            timer.Interval = odpytujCoMinut * 60000;
            timer.Elapsed += new ElapsedEventHandler(this.OnTimer);
            timer.Start();

            autoResetEvent.WaitOne(); // czekam na sygnał zatrzymania wątku
        }

        public void WysylkaWizyt()
        {
            try
            {
                using (connection = new SqlConnection(ConfigurationManager.ConnectionStrings["GaskaConnectionString"].ConnectionString))
                {
                    connection.Open();
                    // Zapytanie 
                    string query = @"SELECT CONVERT(date, [Data rozpoczecia]) as Data
,[ID Wizyty]
,[ID PH]
,[ID Klient]
,cast([Data rozpoczecia] as time) as [Godzina rozpoczecia]
,cast([Data zakonczenia] as time) as[Godzina zakonczenia]

FROM OPENQUERY(gonet,
	'Select
	Z.ID ""ID Wizyty""
	,KH.ID ""ID Klient""
	,O.ID ""ID PH""
	,Z.TERMINROZPOCZECIA ""Data rozpoczecia""
	,Z.TERMINZAKONCZENIA ""Data zakonczenia""
                                          
	FROM KORESPONDENCJADZIENNIK KD 
	JOIN KORESPONDENCJA KO ON KO.ID = KD.IDKORESPONDENCJI
	JOIN ZADANIA Z On Z.IDKORESPONDENCJADZIENNIKROOT = KD.ID                                                          
	JOIN KONTRAHENT KH On KH.ID = KD.IDKONTRAHENTA
	LEFT JOIN SLOWNIK S1 On S1.ID = KH.IDMIASTO
	JOIN OPERATOR O On O.ID = KO.IDOPERATORA

	WHERE KD.USUNIETY = 0
	AND KD.ARC = 0
	AND KO.USUNIETY = 0
	AND KO.ARC = 0
	AND cast(Z.TERMINROZPOCZECIA as date) = cast(''NOW'' as date) - 1 
	'
	)";

                    SqlCommand selectcommand = new SqlCommand(query, connection);
                    using (SqlDataAdapter da = new SqlDataAdapter(selectcommand))
                    {
                        da.Fill(dtWizyty);
                    }
                }
            }
            catch { ZapiszLog("Problem z pobraniem wizyt z bazy danych"); }

            try
            {
                DateTime dzis = DateTime.Now;
                string data = dzis.ToString("yyyyMMddHHmmss");
                string filePath = sciezkaZapisu + data + ".csv";

                using (StreamWriter writer = new StreamWriter(filePath))
                {
                    for (int i = 0; i < dtWizyty.Columns.Count; i++)
                    {
                        writer.Write(dtWizyty.Columns[i]);

                        if (i < dtWizyty.Columns.Count - 1)
                        {
                            writer.Write(",");
                        }
                    }

                    writer.WriteLine();
                    foreach (DataRow row in dtWizyty.Rows)
                    {
                        for (int i = 0; i < dtWizyty.Columns.Count; i++)
                        {
                            writer.Write(row[i].ToString());

                            if (i < dtWizyty.Columns.Count - 1)
                            {
                                writer.Write(",");
                            }
                        }

                        writer.WriteLine();
                    }
                    ZapiszLog("Plik import_wizyta_" + data + ".csv został wygenerowany pomyślnie");
                }
            }
            catch { ZapiszLog("Problem z zapisem pliku Wizyt do csv"); }
        }

        private void ZapiszLog(string tekst)
        {
            DateTime today = DateTime.Today;
            string path = AppDomain.CurrentDomain.BaseDirectory + @"Logs\Logs";
            string pathFull = AppDomain.CurrentDomain.BaseDirectory + @"Logs\Logs\log_" + today.ToString("d") + @".txt";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            using (StreamWriter sw = new StreamWriter(pathFull, true))
            {
                sw.WriteLine(DateTime.Now + " " + tekst);
            }
        }
    }
}
