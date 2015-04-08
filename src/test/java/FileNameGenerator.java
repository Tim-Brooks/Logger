import java.io.File;

/**
 * Created by timbrooks on 4/8/15.
 */
class FileNameGenerator implements FileNameFn {
    private final File root;
    volatile int count = -1;

    public FileNameGenerator(File root) {
        this.root = root;
    }

    @Override
    public String generateFileName() {
        File file = new File(root, "log" + ++count);
        return file.getPath();
    }

    public File getCurrentFile() {
        return new File(root, "log" + (count == -1 ? 0 : count));
    }
}
