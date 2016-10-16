/**
 * Данный класс предназначен для скачивания файла с ftp, его распарсивания и загрузки в БД Oracle.
 * Created by dmitry.arefyev on 27.09.2016.
 */

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.*;
import java.util.Properties;
import java.sql.*;

public class FtpClient {
    //Переменные
    static File conf_dir = new File(System.getProperty("user.dir"), "conf");

    public static void main(String[] args) {
        //Получаем настройки логировани
        Logger log = get_logging_configuration(conf_dir);

        log.info("Запуск приложения");
        log.info("Рабочий каталог: " + System.getProperty("user.dir"));

        //Получаем конфигурацию
        Map conf = new HashMap();
        conf = get_conf(conf_dir, log);

        ArrayList<String> files_array = new ArrayList<String>();
        files_array = get_files(conf, log);
        upload_to_db(files_array, conf, log);
        log.info("Останов приложения");
    }

    private static Logger get_logging_configuration(File conf_dir) {
        /**
         * Считываем конфигурацию логирования
         */
        Logger logger = null;
        InputStream input = null;
        logger = Logger.getLogger(FtpClient.class.getName());
        String filename = "logging.properties";
        File file = new File(conf_dir, filename);
        try {
            input = new FileInputStream(file);
            LogManager.getLogManager().readConfiguration(input);
        }
        catch (Exception e) {
            System.err.println("Ошибка при чтении конфигурации " + filename + ": " + e.getMessage());
        }
        finally {
            if (input!=null) {
                try {
                    input.close();
                }
                catch (Exception e) {
                    System.err.println("Не закрыт файл: " + filename);
                }
            }
        }
        return logger;
    }

    private static Map get_conf(File conf_dir, Logger log) {
        /**
         * Считываем конфигурацию приложения из файла и загоряем в Map
         */
        log.info("Считываем конфиг");
        Properties prop = new Properties();
        InputStream input = null;
        String filename = "config.properties";
        Map conf = new HashMap<String, String>();
        try {
            File file = new File(conf_dir, filename);
            input = new FileInputStream(file);
            System.out.println("Path: " + file);
            prop.load(input);

            Enumeration<?> e = prop.propertyNames();
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                String value = prop.getProperty(key);
                conf.put(key, value);
            }
            log.info("Успешно считан конфиг");
        }
        catch (Exception e) {
            log.severe("Ошибка при чтении конфигурации " + filename + ": " + e.getMessage());
            System.err.println("Ошибка при чтении конфигурации " + filename + ": " + e.getMessage());
        }
        finally {
            if (input!=null) {
                try {
                    input.close();
                    log.info("Поток чтения " + filename + " закрыт");
                }
                catch (Exception e) {
                    log.severe("Не закрыт файл: " + filename + ", ошибка: " + e.getMessage());
                    System.err.println("Не закрыт файл: " + filename + ", ошибка: " + e.getMessage());
                }
            }
        }
        return conf;
    }

    private static ArrayList<String> get_files(Map conf, Logger log) {
        /**
         * Работа с ftp
         */
        log.info("Открываем FTP соединение");
        FTPClient ftpClient = new FTPClient();
        ArrayList<String> files_array = new ArrayList<String>();
        try {
            String user = conf.get("ftp.user").toString() + "@" + conf.get("proxy.user").toString() + "@" + conf.get("ftp.url").toString();
            String pass = conf.get("ftp.password").toString() + "@" + conf.get("proxy.password").toString();

            log.info("Строка соединения: " + user);

            int port = Integer.parseInt(conf.get("proxy.port").toString());
            ftpClient.connect(conf.get("proxy.url").toString(), port);
            ftpClient.login(user, pass);

            files_array = walk_on_ftp(ftpClient, conf, log);
        }
        catch (Exception e) {
            log.severe("Ошибка работы с ftp: " + e.getMessage());
            System.err.println("Ошибка работы с ftp: " + e.getMessage());
        }
        finally {
            if (ftpClient!=null) {
                try {
                    ftpClient.logout();
                    ftpClient.disconnect();
                    log.info("FTP соединение закрыто");
                } catch (IOException e) {
                    log.severe("Ошибка работы с ftp: " + e.getMessage());
                    System.err.println("Ошибка работы с ftp: " + e.getMessage());
                }
            }
        }
        return files_array;
    }


    private static ArrayList<String> walk_on_ftp(FTPClient ftpClient, Map conf, Logger log) {
        /**
         * Обрабатываем с каталогом
         */
        ArrayList<String> files_array = new ArrayList<String>();
        try {
            String[] ftp_dirs = conf.get("ftp.dirs").toString().split(",");
            for (int i=0; i < ftp_dirs.length; i++) {
                log.info("Работаем с каталогом: " + ftp_dirs[i]);
                System.out.println("Работаем с каталогом: " + ftp_dirs[i]);
                ftpClient.changeWorkingDirectory(ftp_dirs[i]);
                files_array = get_path(ftpClient, ftp_dirs[i], "null", files_array, conf, log);
                log.info("Данные с ftp получены");
            }
        }
        catch (Exception e) {
            log.severe("Ошибка работы с каталогом " + e.getMessage());
        }
        return files_array;
    }

    private static ArrayList get_path(FTPClient ftpClient, String folder, String sub_folder, ArrayList<String> files_array, Map conf, Logger log) {
        /**
         * Обрабатываем содержимое в рекурсии
         */
        //Получаем список файлов
        try {
            if (!sub_folder.equals("null")) {
                File new_folder = new File(folder, sub_folder);
                folder = new_folder.toString();
                ftpClient.changeWorkingDirectory(sub_folder);
                System.out.println("Работает с подкаталогом: " + folder);
                log.info("Работает с подкаталогом: " + folder);
            }
            FTPFile[] files = ftpClient.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    get_path(ftpClient, folder, files[i].getName(), files_array, conf, log);
                } else {
                    String extension = files[i].getName().substring(files[i].getName().lastIndexOf("."));
                    if (!extension.equals(".md5")) {
                        File out_file = new File(folder, files[i].getName());
                        files_array.add(out_file.toString());
                    }
                }
            }
            ftpClient.changeToParentDirectory();

        } catch (StringIndexOutOfBoundsException e) {
            //Не нужный файл.
            //Вышли за индекс.
            log.severe("Не нужный файл. " + " Вышли за индекс:" + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            log.severe("Ошибка обхода подкаталога: " + folder);
            System.out.println("Ошибка обхода подкаталога: " + folder);
        }
        return files_array;
    }

    private static void upload_to_db(ArrayList<String> files_array, Map conf, Logger log) {
        /**
         * Загружаем содержимое в БД.
         */
        Connection connection = null;
        String url = (String) conf.get("db.url");
        String name = (String) conf.get("db.user");
        String password = (String) conf.get("db.password");
        String table = (String) conf.get("db.table");
        log.info("Получена конфигурация подключения к БД " + url);
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            log.info("Драйвер подключен");
            connection = DriverManager.getConnection(url, name, password);
            if (!connection.isClosed()) {
                log.info("Подключились к БД");
            }

            clear_table(connection, table, conf, log);
            //PreparedStatement: предварительно компилирует запросы,
            //которые могут содержать входные параметры
            log.info("Таблица для загрузки: " + table);
            PreparedStatement preparedStatement = null;
            preparedStatement = connection.prepareStatement("INSERT INTO " + table + " (url) values(?)");
            for (int i=0; i < files_array.size(); i++) {
                preparedStatement.setString(1, files_array.get(i));
                preparedStatement.executeUpdate();
                if (i%1000 == 0) {
                    connection.commit();
                    log.info("Commit после " + i + " записей");
                }
            }
            log.info("Загрузка в таблицу завершена");

        } catch (ClassNotFoundException e) {
            log.severe("Ошибка инициализации класса для БД " + e.getMessage());
        } catch (SQLException e) {
            log.severe("Ошибка выполнения операции в БД " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.commit();
                    connection.close();
                    log.info("Соединение с БД закрыто");
                }
                catch (Exception e) {
                    log.severe("Не закрыто соединение с БД: " + e.getMessage());
                }
            }
        }
    }

    private static void clear_table(Connection connection, String table, Map conf, Logger log) {
        /**
         * Чистим табличку
         */
        //Выполянем Statement запрос (без параметров)
        Statement statement = null;

        try {
            statement = connection.createStatement();
            //Выполняем
            ResultSet result = statement.executeQuery("truncate table " + table);
            log.info("truncate таблицы: " + table);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
