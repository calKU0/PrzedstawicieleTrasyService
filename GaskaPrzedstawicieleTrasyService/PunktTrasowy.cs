using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace GaskaPrzedstawicieleTrasyService
{
    internal class PunktTrasowy : IDisposable
    {
        public string Data { get; set; }
        public int IdPh { get; set; }
        public int Kolejnosc { get; set; }
        public int IdKlient { get; set; }
        public string NazwaKlient { get; set; }
        public string GodzinaRozpoczecia { get; set; }
        public int CzasWizyty { get; set; }

        public void ZalozZadanie()
        {
            try
            {
                //formatowanie daty i godziny rozpoczęcia
                //Zmieniamy godzine rozpoczęcia w zależności od kolejności, aby w GoNecie było zadanie od 8-16 co godzinę
                if (Kolejnosc == 1) { GodzinaRozpoczecia = "08:00:00"; }
                if (Kolejnosc == 2) { GodzinaRozpoczecia = "09:00:00"; }
                if (Kolejnosc == 3) { GodzinaRozpoczecia = "10:00:00"; }
                if (Kolejnosc == 4) { GodzinaRozpoczecia = "11:00:00"; }
                if (Kolejnosc == 5) { GodzinaRozpoczecia = "12:00:00"; }
                if (Kolejnosc == 6) { GodzinaRozpoczecia = "13:00:00"; }
                if (Kolejnosc == 7) { GodzinaRozpoczecia = "14:00:00"; }
                if (Kolejnosc == 8) { GodzinaRozpoczecia = "15:00:00"; }
                if (Kolejnosc == 9) { GodzinaRozpoczecia = "16:00:00"; }

                SqlConnection connection;
                string datetimeString = $"{Data} {GodzinaRozpoczecia}";
                string inputFormat = "yyyyMMdd HH:mm:ss";
                DateTime datetime = DateTime.ParseExact(datetimeString, inputFormat, System.Globalization.CultureInfo.InvariantCulture);
                string outputFormat = "yyyy-MM-dd HH:mm:ss";
                string formattedGodzinaRozpoczecia = datetime.ToString(outputFormat);

                //Dodanie długości wizyty, aby obliczyć godzinę zakończenia
                DateTime incrementedDateTime = datetime.AddMinutes(60);
                string formattedGodzinaZakonczenia = incrementedDateTime.ToString(outputFormat);

                using (connection = new SqlConnection(ConfigurationManager.ConnectionStrings["GaskaConnectionString"].ConnectionString))
                {
                    string query = @"
                IF (SELECT TOP 1 TERMINZAKONCZENIA FROM OPENQUERY(gonet,'SELECT TERMINZAKONCZENIA FROM ZADANIA WHERE IDZLECENIODAWCY = 78 AND USUNIETY = 0 AND ARC = 0') ORDER BY TERMINZAKONCZENIA DESC) <= @dataZakonczenia OR NOT EXISTS (SELECT TOP 1 TERMINZAKONCZENIA FROM OPENQUERY(gonet,'SELECT TERMINZAKONCZENIA FROM ZADANIA WHERE IDZLECENIODAWCY = 78 AND USUNIETY = 0 AND ARC = 0') ORDER BY TERMINZAKONCZENIA DESC)
                BEGIN
                INSERT INTO openquery(gonet,'SELECT TERMINROZPOCZECIA
                ,TERMINZAKONCZENIA
                ,STATUS
                ,IDGRUPY
                ,UKONCZONO
                ,IDKONTRAHENTA
                ,PRIORYTET
                ,WYKONANE
                ,EDIT
                ,DLAGRUPY
                ,IDTYPZADANIA
                ,IDRODZAJZADANIA
                ,IDZLECENIODAWCY
                ,IDZLECENIOBIORCY
                ,DATAUKONCZENIA
                ,PRYWATNE
                ,DATAWPISU
                ,IDOPERATORA
                ,CYKLICZNE
                ,COILEDNI
                ,USUNIETY
                ,IDETAPU
                ,INFO
                ,ARC
                ,SYNC
                ,EXXID
                ,IDODDZIALU
                ,ZALACZNIKI
                ,FAKTSTART
                ,FAKTKONIEC
                ,FAKTTIMER
                ,TYPCYKLZADANIA
                ,WARTOSC
                ,IDKORESPONDENCJADZIENNIKROOT
                ,IDINSTALACJI
                ,IDURZADZENIA
                ,IDADRESATA
                ,IDKOLORU
                ,OPCJEPOWIADOMIEN
                ,LAT
                ,LNG
                ,IDZADANIAROOT
                ,IDTABELI
                ,IDREKORDU
                ,ROZLTIMER
                FROM ZADANIA') 
                SELECT @dataRozpoczecia, @dataZakonczenia,0,-1,0,ID,0,0,0,0,-1,5611,78,@idPH,'1899-12-29 00:00:00.0000000',0,@data,@idPH,0,-1,0,-1,'',0,1,-1,1,0,'1899-12-29 00:00:00.0000000','1899-12-29 00:00:00.0000000',0,-1,0.00,-1,-1,-1,-1,-1,1,NULL,NULL,-1,-1,-1,0
                from openquery(gonet, 'SELECT k.ID ""ID"" FROM kontrahent k WHERE K.USUNIETY = 0 AND K.ARC = 0 AND k.SKROTNAZWY = ''" + NazwaKlient + "''') END";

                    connection.Open();
                    SqlCommand insertCommand = new SqlCommand(query, connection);
                    insertCommand.Parameters.AddWithValue("@dataRozpoczecia", formattedGodzinaRozpoczecia);
                    insertCommand.Parameters.AddWithValue("@dataZakonczenia", formattedGodzinaZakonczenia);
                    insertCommand.Parameters.AddWithValue("@idPH", IdPh);
                    insertCommand.Parameters.AddWithValue("@data", DateTime.Now.ToString(outputFormat));
                    //insertCommand.Parameters.AddWithValue("@nazwaKlient", nazwaKlient);
                    insertCommand.ExecuteNonQuery();
                    ZapiszLog($"Dodano zadanie o atrybutach: Data: {Data} idPH: {IdPh} Kolejnosc: {Kolejnosc} idKlient: {IdKlient} Nazwa klienta: {NazwaKlient} Godzina wizyty: {GodzinaRozpoczecia} Czas wizyty: {CzasWizyty}");
                    connection.Close();
                }
            }
            catch (Exception ex) { ZapiszLog($"Wystąpił problem z tworzeniem zadania o atrybutach: Data: {Data} idPH: {IdPh} Kolejnosc: {Kolejnosc} idKlient: {IdKlient} Nazwa klienta: {NazwaKlient} Godzina wizyty: {GodzinaRozpoczecia} Czas wizyty: {CzasWizyty} {ex}"); }
        }

        public static void ArchiwizujZadania()
        { 
            try
            {
                SqlConnection connection;
                string outputFormat = "yyyy-MM-dd HH:mm:ss";
                using (connection = new SqlConnection(ConfigurationManager.ConnectionStrings["GaskaConnectionString"].ConnectionString))
                {
                    string query = @"UPDATE gonet
                    SET gonet.ARC = 1 , gonet.USUNIETY = 0
                    from openquery
                    (gonet,'SELECT ID, ARC, USUNIETY, TERMINROZPOCZECIA FROM ZADANIA WHERE IDZLECENIODAWCY = 78 AND ARC = 0 AND USUNIETY = 0'
                    ) gonet
                    where gonet.TERMINROZPOCZECIA >= @data";

                    connection.Open();
                    SqlCommand updateCommand = new SqlCommand(query, connection);
                    updateCommand.Parameters.AddWithValue("@data", DateTime.Now.ToString(outputFormat));
                    int liczbaZmian = updateCommand.ExecuteNonQuery();
                    connection.Close();
                    
                    ZapiszLog($"Zarchiwizowano {liczbaZmian} zadań RoutePlus");
                }
            }
            catch (Exception ex) { ZapiszLog("Wystąpił błąd przy archiwizowaniu zadań: " + ex); }
        }

        public static void ZapiszLog(string tekst)
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
                sw.WriteLine(DateTime.Now + " " + tekst + "\n");
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        /// <summary>
        /// Is this instance disposed?
        /// </summary>
        protected bool Disposed { get; private set; }

        /// <summary>
        /// Dispose worker method. See http://coding.abel.nu/2012/01/disposable
        /// </summary>
        /// <param name="disposing">Are we disposing? 
        /// Otherwise we're finalizing.</param>
        protected virtual void Dispose(bool disposing)
        {
            Disposed = true;
        }
    }
}
