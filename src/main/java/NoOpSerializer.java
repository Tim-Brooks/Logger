/**
 * Created by timbrooks on 4/8/15.
 */
public class NoOpSerializer implements LogSerializer {
    @Override
    public String serialize(Object object) {
        return object.toString() + "\n";
    }
}
