import java.sql.Connection
import java.sql.SQLException

class GrupoDao (private val c: Connection) {
    companion object {
        private const val SCHEMA = "default"
        private const val TABLE = "GRUPOS"
        private const val TRUNCATE_TABLE_GRUPOS_SQL = "TRUNCATE TABLE GRUPOS"
        private const val CREATE_TABLE_GRUPOS_SQL =
            "CREATE TABLE GRUPOS (grupoid INT NOT NULL AUTO_INCREMENT,grupodesc VARCHAR(100) NOT NULL,mejorposCTFid INT,PRIMARY KEY (grupoid));"
        private const val INSERT_GRUPOS_SQL =
            "INSERT INTO GRUPOS" + "  (grupoid, grupodesc, mejorposCTFid) VALUES " + " (?, ?, ?);"
        private const val SELECT_ALL_GRUPOS = "select * from GRUPOS"
        private const val SELECT_GRUPOS_BY_ID = "select grupoid, grupodesc, mejorposCTFid from GRUPOS where grupoid =?"
        private const val DELETE_GRUPOS_SQL = "delete from GRUPOS where grupoid = ?;"
        private const val UPDATE_GRUPOS_SQL =
            "update GRUPOS set grupoid = ?,grupodesc= ?, mejorposCTFid =? where grupoid = ?;"
    }
    fun prepareTable() {
        val metaData = c.metaData
        val rs = metaData.getTables(null, SCHEMA, TABLE, null)

        if (!rs.next()) createTable() else truncateTable()
    }

    private fun truncateTable() { //Sirve para eliminar los datos de la tabla
        println(TRUNCATE_TABLE_GRUPOS_SQL)
        try {
            c.createStatement().use { st ->
                st.execute(TRUNCATE_TABLE_GRUPOS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }
    private fun createTable() {
        println(CREATE_TABLE_GRUPOS_SQL)
        try {

            c.createStatement().use { st ->
                st.execute(CREATE_TABLE_GRUPOS_SQL)
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun selectById(id: Int): Grupo? {
        var user: Grupo? = null

        try {
            c.prepareStatement(SELECT_GRUPOS_BY_ID).use { st ->
                st.setInt(1, id)
                println(st)
                val rs = st.executeQuery()
                while (rs.next()) {
                    val grupoid = rs.getInt("grupoid")
                    val grupodesc = rs.getString("grupodesc")
                    val mejorposCTFid = rs.getInt("mejorposCTFid")
                    user = Grupo(grupoid,grupodesc,mejorposCTFid)
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return user
    }

    fun insertGrupo(grupo: Grupo) {
        println(INSERT_GRUPOS_SQL)
        try {
            c.prepareStatement(INSERT_GRUPOS_SQL).use { st ->
                st.setInt(1, grupo.grupoid)
                st.setString(2, grupo.grupodesc)
                st.setInt(3, grupo.mejorposCTFid)
                println(st)
                st.executeUpdate()
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
    }

    fun selectAllGrupos(): List<Grupo> {

        val grupos: MutableList<Grupo> = ArrayList()
        try {
            c.prepareStatement(SELECT_ALL_GRUPOS).use { st ->
                println(st)
                val rs = st.executeQuery()
                while (rs.next()) {
                    val grupoid = rs.getInt("GRUPOID")
                    val grupodesc = rs.getString("GRUPODESC")
                    val mejorpos = rs.getInt("MEJORPOSCTFID")
                    grupos.add(Grupo(grupoid, grupodesc,mejorpos))
                }
            }

        } catch (e: SQLException) {
            printSQLException(e)
        }
        return grupos
    }
    fun updateGrupo(grupo:Grupo): Boolean {
        var rowUpdated = false

        try {
            c.prepareStatement(UPDATE_GRUPOS_SQL).use { st ->
                st.setInt(1,grupo.grupoid)
                st.setInt(2,grupo.mejorposCTFid)
                rowUpdated = st.executeUpdate() > 0
            }
            c.commit()
        } catch (e: SQLException) {
            printSQLException(e)
        }
        return rowUpdated
    }


    private fun printSQLException(ex: SQLException) { //Esta funcion privada es para mostrar si ha habido un error
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