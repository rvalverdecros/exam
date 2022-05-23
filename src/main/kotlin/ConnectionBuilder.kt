import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException


class ConnectionBuilder { //Aqui se encuentra los datos de conexion de la base de datos
    lateinit var connection: Connection
    private val jdbcURL = "jdbc:h2:file:/~/pruebaex"
    private val jdbcUsername = "user"
    private val jdbcPassword = "user"


    init {
        try {
            connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword)
            connection.autoCommit=false
        } catch (e: SQLException) {
            // TODO Auto-generated catch block
            e.printStackTrace()
        } catch (e: ClassNotFoundException) {
            // TODO Auto-generated catch block
            e.printStackTrace()
        }
    }

}