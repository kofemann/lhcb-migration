package org.dcache.migraion.lhcb;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws Exception {

        var config = loadProperties(args);

        var chimeraDb = getDbConnection("chimera", config);
        var spacemgrDb = getDbConnection("spacemgr", config);

        try (chimeraDb; spacemgrDb) {
            Migration migration = new Migration(
                    config.getProperty("path.src"),
                    config.getProperty("path.dest"),
                    Integer.parseInt(config.getProperty("path.owner")),
                    Integer.parseInt(config.getProperty("path.group")),
                    chimeraDb, spacemgrDb);
            migration.run(Splitter.on(',').omitEmptyStrings().trimResults().splitToList(config.getProperty("tokens")));
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    private static HikariDataSource getDbConnection(String prefix, Properties cfg) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(cfg.getProperty(prefix + ".url"));
        config.setUsername(cfg.getProperty(prefix + ".user"));
        config.setPassword(cfg.getProperty(prefix + ".pass"));
        config.setMaximumPoolSize(3);

        return new HikariDataSource(config);
    }

    private static Properties loadProperties(String[] args) throws FileNotFoundException, IOException {

        String properties[] = {
                "chimera.url",
                "chimera.user",
                "chimera.pass",
                "spacemgr.url",
                "spacemgr.user",
                "spacemgr.pass",
                "tokens",
                "path.src",
                "path.dest",
                "path.owner",
                "path.group"
        };

        var config = new Properties();

        var configFile = args.length > 0 ? args[0] : "migration.properties";

        var file = new File(configFile);
        if (!file.exists()) {
            System.err.println("Property file " + file + " doesn't exists!");
            System.exit(3);
        }
        config.load(Files.newReader(file, StandardCharsets.UTF_8));

        for (String property : properties) {

            if (!config.containsKey(property)) {
                System.err.println("Property " + property + " not specified!");
                System.exit(1);
            }

            if (Strings.isNullOrEmpty(config.getProperty(property))) {
                System.err.println("Property " + property + " is empty!");
                System.exit(1);
            }
        }

        return config;
    }
}
