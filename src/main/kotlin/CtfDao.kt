import java.sql.Connection
import java.sql.SQLException

class CtfDao(private val c: Connection) {
    companion object {
        private const val SCHEMA = "default"
        private const val TABLE = "CTFS"
        private const val TRUNCATE_TABLE_CTFS_SQL = "TRUNCATE TABLE CTFS"
        private const val CREATE_TABLE_CTFS_SQL =
            "CREATE TABLE CTFS (CTFid INT NOT NULL,grupoid INT NOT NULL,puntuacion INT NOT NULL,PRIMARY KEY (CTFid,grupoid));"
        private const val INSERT_CTFS_SQL = "INSERT INTO CTFS" + "  (CTFid, grupoId, puntuacion) VALUES " + " (?, ?, ?);"
        private const val SELECT_ALL_CTFS = "select * from CTFS"
        private const val DELETE_CTFS_SQL = "delete from CTFS where CTFid = ? AND grupoId = ?;"
    }
    fun prepareTable() {
        val metaData = c.metaData
        val rs = metaData.getTables(null, SCHEMA, TABLE, null)

        if (!rs.next()) createTable() else truncateTable()
    }

    private fun truncateTable() {
        println(TRUNCATE_TABLE_CTFS_SQL)
        try {
            c.createStatement().use { st ->
                st.execute(TRUNCATE_TABLE_CTFS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    private fun createTable() {
        println(CREATE_TABLE_CTFS_SQL)
        try {

            c.createStatement().use { st ->
                st.execute(CREATE_TABLE_CTFS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun insertCTFs(ctf: Ctf ) {
        println(INSERT_CTFS_SQL)
        try {
            c.prepareStatement(INSERT_CTFS_SQL).use { st ->
                st.setInt(1, ctf.CTFid)
                st.setInt(2, ctf.grupoId)
                st.setInt(3, ctf.puntuacion)
                println(st)
                st.executeUpdate()
            }
            //Commit the change to the database
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun selectAllCTFS(): List<Ctf> {

        val ctfs: MutableList<Ctf> = ArrayList()
        try {
            c.prepareStatement(SELECT_ALL_CTFS).use { st ->
                println(st)
                val rs = st.executeQuery()
                while (rs.next()) {
                    val CTFid = rs.getInt("CTFid")
                    val grupoId = rs.getInt("grupoId")
                    val puntuacion = rs.getInt("puntuacion")
                    ctfs.add(Ctf(CTFid, grupoId,puntuacion))
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return ctfs
    }

    fun deleteCTFSById(id: Int, seg: Int): Boolean {
        var rowDeleted = false

        try {
            c.prepareStatement(DELETE_CTFS_SQL).use { st ->
                st.setInt(1, id)
                st.setInt(2, seg)
                rowDeleted = st.executeUpdate() > 0
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
        return rowDeleted
    }

    private fun printSQLException(ex: SQLException) {
        for (e in ex) {
            if (e is SQLException) {
                e.printStackTrace(System.err)
                System.err.println("SQLState: " + e.sqlState)
                System.err.println("Error Code: " + e.errorCode)
                System.err.println("Message: " + e.message)
                var t = ex.cause
                while (t != null) {
                    println("Cause: $t")
                    t = t.cause
                }
            }
        }
    }
}