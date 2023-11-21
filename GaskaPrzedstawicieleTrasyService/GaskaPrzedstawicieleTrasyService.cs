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
using Renci.SshNet;

namespace GaskaPrzedstawicieleTrasyService
{
    public partial class GaskaPrzedstawicieleTrasyService : ServiceBase
    {
        DataTable dtWizyty = new DataTable();
        DataTable dtKlienci = new DataTable();
        System.Timers.Timer timer = new System.Timers.Timer();
        string sftpHost = ConfigurationManager.AppSettings["SftpHostname"];
        int sftpPort = int.Parse(ConfigurationManager.AppSettings["SftpPort"]);
        string sftpUsername = ConfigurationManager.AppSettings["SftpUsername"];
        string sftpPassword = ConfigurationManager.AppSettings["SftpPassword"];
        string godzinaWysylki = ConfigurationManager.AppSettings["Godzina wysylki"];
        string sftpFolderPath = ConfigurationManager.AppSettings["SftpFolderPath"];
        int odpytujCoMinut = int.Parse(ConfigurationManager.AppSettings["Co ile minut odpytywac"]);
        string czyJuzWykonano = "";
        private static AutoResetEvent autoResetEvent = new AutoResetEvent(false);
        string data;
        SqlConnection connection;

        public GaskaPrzedstawicieleTrasyService()
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
            try
            {
                string aktualnaGodzina = DateTime.Now.Hour.ToString();
                string data = DateTime.Now.ToShortDateString();

                if (aktualnaGodzina == godzinaWysylki && czyJuzWykonano != data) // jesli juz wykonalem operacje o zadanej godzinie w dzisiejszym dniu to juz jej nie wykonam ponownie
                {
                    dtWizyty.Clear();
                    dtKlienci.Clear();
                    Thread threadWysylka = new Thread(Wysylka);
                    threadWysylka.Start();
                    czyJuzWykonano = data;
                }
            }
            catch (Exception ex) { ZapiszLog(ex.ToString()); }


        }

        private void Watek()
        {
            try
            {
                timer.Interval = odpytujCoMinut * 60000;
                timer.Elapsed += new ElapsedEventHandler(this.OnTimer);
                timer.Start();

                autoResetEvent.WaitOne(); // czekam na sygnał zatrzymania wątku
            }
            catch (Exception ex) { ZapiszLog(ex.ToString()); }
        }

        public void Wysylka()
        {
            try
            {
                //1. Wysyłamy plik z Wizytami
                WyslijWizyty();
                //2. Wysyłamy plik z Klientami
                WyslijKlientow();
            }
            catch (Exception ex) {ZapiszLog(ex.ToString()); }
        }

        public void WyslijWizyty()
        {
            try
            {
                using (connection = new SqlConnection(ConfigurationManager.ConnectionStrings["GaskaConnectionString"].ConnectionString))
                {
                    string query = @"SELECT CONVERT(date, [Data rozpoczecia]) as Data
,[ID Wizyty]
,[ID PH]
,[ID Klient]
,[Klient Nazwa]
,cast([Data rozpoczecia] as time) as [Godzina rozpoczecia]
,cast([Data zakonczenia] as time) as[Godzina zakonczenia]

                                    FROM OPENQUERY(gonet,
	                                    'Select
	                                    Z.ID ""ID Wizyty""
                                        ,KH.ID ""ID Klient""
	                                    ,KH.SKROTNAZWY ""Klient Nazwa""
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
                                        AND kh.IDMANAGERA in (51,58,59,68) -- Przedstawiciele handlowi
	                                    AND cast(KO.DATAAKCJI as date) = cast(''NOW'' as date) - 1 -- Data zamknięcia wczoraj
										AND KO.IDTYPAKCJI = 4 -- Wizyta'
	                                    )

		SELECT * FROM OPENQUERY(gonet,'SELECT * FROM KORESPONDENCJA K WHERE K.ID IN (606591,611831)')";
                    connection.Open();
                    SqlCommand selectcommand = new SqlCommand(query, connection);
                    using (SqlDataAdapter da = new SqlDataAdapter(selectcommand))
                    {
                        da.Fill(dtWizyty);
                    }
                    connection.Close();
                }
            }
            catch {ZapiszLog("Problem z pobraniem wizyt z bazy danych"); }

            DateTime dzis = DateTime.Now;
            data = dzis.ToString("yyyyMMddHHmmss");
            string pathWizyta = AppDomain.CurrentDomain.BaseDirectory + @"Wizyty";
            string filePathWizyta = AppDomain.CurrentDomain.BaseDirectory + @"Wizyty\import_wizyta_" + data + ".csv";
            GenerujCSV(pathWizyta, filePathWizyta, dtWizyty);
            WyslijPlik(filePathWizyta, "import_wizyta_" + data + ".csv");
        }

        public void WyslijKlientow()
        {
            try
            {
                using (connection = new SqlConnection(ConfigurationManager.ConnectionStrings["GaskaConnectionString"].ConnectionString))
                {
                    string query = @"
select distinct
knt_gidnumer as [Id Klient]
,knt_akronim as [Nazwa]
,knt_kraj as [Kraj]
,knt_kodp as [Kod]
,knt_miasto as [Miejscowość]
,knt_ulica as [Ulica]
,case when KtO_PrcNumer = 382 then 51
when KtO_PrcNumer = 1491 then 68
when KtO_PrcNumer = 1426 then 59
else 58 end as [ID PH]
,ilo.atr_wartosc as [Ilość Odwiedzin]

from cdn.KntKarty
join cdn.Rejony on REJ_Id = KnT_RegionCRM
join cdn.KntOpiekun on REJ_Id=KtO_KntNumer and KtO_Glowny = 0 and KtO_PrcNumer in (1425,1426,1491,382)
join cdn.atrybuty ilo on Knt_GIDNumer=ilo.Atr_ObiNumer and ilo.Atr_OBITyp=32 AND ilo.Atr_OBISubLp=0 and ilo.atr_atkid = 459 and ilo.Atr_Wartosc <> 0 
left join cdn.atrybuty co on Knt_GIDNumer=co.Atr_ObiNumer and co.Atr_OBITyp=32 AND co.Atr_OBISubLp=0 and co.atr_atkid = 470

where co.Atr_Wartosc is NULL or co.Atr_Wartosc = 'TAK'
UNION ALL
select distinct
kna_gidnumer as [Id Klient]
,kna_akronim as [Nazwa]
,kna_kraj as [Kraj]
,kna_kodp as [Kod]
,kna_miasto as [Miejscowość]
,kna_ulica as [Ulica]
,case when KtO_PrcNumer = 382 then 51
when KtO_PrcNumer = 1491 then 68
when KtO_PrcNumer = 1426 then 59
else 58 end as [ID PH]
,ilo.atr_wartosc as [Ilość Odwiedzin]


from cdn.KntAdresy
join cdn.Rejony on REJ_Id = KnA_RegionCRM
join cdn.KntOpiekun on REJ_Id=KtO_KntNumer and KtO_Glowny = 0
join cdn.PrcKarty on Prc_GIDNumer=KtO_PrcNumer
join cdn.atrybuty ilo on KnA_GIDNumer=ilo.Atr_ObiNumer and ilo.atr_atkid = 459 and ilo.Atr_Wartosc <> 0
left join cdn.atrybuty co on KnA_GIDNumer=co.Atr_ObiNumer and co.atr_atkid = 470

where (co.Atr_Wartosc is NULL or co.Atr_Wartosc = 'TAK') and KnA_AdresBank = 1 and KtO_PrcNumer in (1425,1426,1491,382)";
                    connection.Open();
                    SqlCommand selectcommand = new SqlCommand(query, connection);
                    using (SqlDataAdapter da = new SqlDataAdapter(selectcommand))
                    {
                        da.Fill(dtKlienci);
                    }
                    connection.Close();
                }
            }
            catch (Exception ex) { ZapiszLog("Problem z pobraniem klientów z bazy danych\n" + ex); }

            DateTime dzis = DateTime.Now;
            data = dzis.ToString("yyyyMMddHHmmss");
            string pathKlienci = AppDomain.CurrentDomain.BaseDirectory + @"Klienci";
            string filePathKlienci = AppDomain.CurrentDomain.BaseDirectory + @"Klienci\import_klient_" + data + ".csv";

            GenerujCSV(pathKlienci, filePathKlienci, dtKlienci);
            WyslijPlik(filePathKlienci, "import_klient_" + data + ".csv");
        }

        public void GenerujCSV(string path, string filePath, DataTable dt)
        {
            try
            {
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
                using (StreamWriter writer = new StreamWriter(filePath))
                {
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        writer.Write(dt.Columns[i]);
                        if (i < dt.Columns.Count - 1)
                        {
                            writer.Write("|");
                        }
                    }
                    writer.WriteLine();
                    foreach (DataRow row in dt.Rows)
                    {
                        for (int i = 0; i < dt.Columns.Count; i++)
                        {
                            writer.Write(row[i].ToString());
                            if (i < dt.Columns.Count - 1)
                            {
                                writer.Write("|");
                            }
                        }
                        writer.WriteLine();
                    }
                }
                ZapiszLog("Plik " + filePath + " został wygenerowany pomyślnie");
            }
            catch (Exception ex) { ZapiszLog("Problem z zapisem do pliku " + filePath + "\n"+ ex); }
        }

        public void WyslijPlik(string filePath, string fileName)
        {
            try
            {
                using (SftpClient sftpClient = new SftpClient(sftpHost, sftpPort, sftpUsername, sftpPassword))
                {
                    sftpClient.Connect();
                    sftpClient.ChangeDirectory(sftpFolderPath);
                    using (FileStream fileStream = new FileStream(filePath, FileMode.Open))
                    {
                        sftpClient.UploadFile(fileStream, fileName);
                    }
                    sftpClient.Disconnect();
                }
                ZapiszLog("Plik " + fileName + " został wysłany na serwer sftp pomyślnie");
            }
            catch(Exception ex) { ZapiszLog("Problem z wysyłką pliku " + filePath + "na SFTP\n" + ex); }
        }

        public void ZapiszLog(string tekst)
        {
            DateTime today = DateTime.Today;
            string path = AppDomain.CurrentDomain.BaseDirectory + @"Logs";
            string pathFull = AppDomain.CurrentDomain.BaseDirectory + @"Logs\log_" + today.ToString("d") + @".txt";
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
