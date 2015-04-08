/**
 * Created by timbrooks on 4/8/15.
 */
public class DefaultErrorHandler implements ErrorCallback {
    @Override
    public void error(Throwable error) {
        error.printStackTrace();
    }
}
