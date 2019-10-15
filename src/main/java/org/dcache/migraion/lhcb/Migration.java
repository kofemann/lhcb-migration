package org.dcache.migraion.lhcb;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hazelcast.core.Hazelcast;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

public class Migration {

    private final static Logger LOGGER = LoggerFactory.getLogger(Migration.class);

    private final JdbcTemplate jdbc;
    private final JdbcFs fs;
    private final String src;
    private final String dest;

    private final LoadingCache<String, FsInode> dirCacheOld;
    private final LoadingCache<String, FsInode> dirCacheNew;

    public Migration(String src, String dest, DataSource chimeraDb, DataSource spaceMgrDb) throws ChimeraFsException, SQLException {

        jdbc = new JdbcTemplate(spaceMgrDb);
        this.src = src;
        this.dest = dest;

        PlatformTransactionManager txManager = new DataSourceTransactionManager(chimeraDb);
        fs = new JdbcFs(chimeraDb, txManager, Hazelcast.newHazelcastInstance());

        dirCacheOld = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .build(new CacheLoader<String, FsInode>() {
                    @Override
                    public FsInode load(String k) throws Exception {
                        return fs.path2inode(k);
                    }
                }
                );

        dirCacheNew = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .build(new CacheLoader<String, FsInode>() {
                    @Override
                    public FsInode load(String k) throws Exception {

                        try {
                            return fs.path2inode(k);
                        } catch (FileNotFoundHimeraFsException e) {
                            File f = new File(k);
                            if (f.getParent() != null) {
                                dirCacheNew.get(f.getParent());
                            }
                            return fs.mkdir(k);
                        }
                    }
                }
                );

    }

    public void run() throws ChimeraFsException {

        var treeRoot = fs.path2inode(dest);

        Map<Long, String> map = getSpaceTokens();
        System.out.println();
        System.out.println("=== processing space tokens ===");
        System.out.println();

        for (Map.Entry<Long, String> tokens : map.entrySet()) {

            System.out.print(tokens.getValue());
            progresInit();
            jdbc.query("SELECT pnfsid FROM srmspacefile where spacereservationid = ?",
                    ps -> {
                        ps.setLong(1, tokens.getKey());
                    },
                    rs -> {

                        try {
                            String pnfsid = rs.getString("pnfsid");
                            if (Strings.isNullOrEmpty(pnfsid)) {
                                return;
                            }
                            var inode = fs.id2inode(pnfsid, FileSystemProvider.StatCacheOption.NO_STAT);
                            var path = fs.inode2path(inode, treeRoot);

                            File newFile = new File(src + "/" + tokens.getValue() + "/" + path);
                            File oldFile = new File(dest + path);
                            String newDirname = newFile.getParentFile().toString();
                            String oldDirname = oldFile.getParentFile().toString();
                            String name = oldFile.getName();
                            FsInode destDir = dirCacheNew.get(newDirname);
                            FsInode srcDir = dirCacheOld.get(oldDirname);

                            progresTick();
                            fs.rename(inode, srcDir, name, destDir, name);
                        } catch (ChimeraFsException | ExecutionException ex) {
                            LOGGER.error("Failed to discover path: {}", ex.getMessage());
                        }
                    });

            progresFinish();
        }

        System.out.println();
        System.out.println("===         Done!           ===");
        System.out.println();
    }

    private Map<Long, String> getSpaceTokens() throws DataAccessException {
        Map<Long, String> map = new HashMap<>();
        jdbc.query("SELECT id, description FROM srmspace",
                rs -> {
                    map.put(rs.getLong("id"), rs.getString("description"));
                }
        );
        return map;
    }

    private final char[] bar = {'-', '\\', '|', '/'};
    int i;

    private void progresInit() {
        i = 0;
        System.out.print("  ");
    }

    private void progresFinish() {
        System.out.print("\b");
        System.out.println(i + " files.");
    }

    private void progresTick() {
        System.out.print("\b");
        System.out.print(bar[i++ % bar.length]);
    }

}
