import java.io.File;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.RefSpec;

/**
 * Spike S1 client harness for JGit.
 *
 * <p>{@code S1JGit <url> <repo-dir>} fetches {@code +refs/heads/main:refs/remotes/origin/main}
 * from url into an existing repository. JGit reads {@code protocol.version} from the
 * repository's config. {@code S1JGit push <url> <repo-dir>} pushes {@code +refs/heads/main}
 * to url instead (for push_order.py). Exits non-zero with JGit's error on failure.
 */
public class S1JGit {
  public static void main(String[] args) {
    if (args.length == 3 && args[0].equals("push")) {
      try (Git git = Git.open(new File(args[2]))) {
        git.push()
            .setRemote(args[1])
            .setRefSpecs(new RefSpec("+refs/heads/main:refs/heads/main"))
            .call()
            .forEach(r -> r.getRemoteUpdates().forEach(u -> {
              if (u.getStatus() != org.eclipse.jgit.transport.RemoteRefUpdate.Status.OK) {
                System.err.println("error: " + u.getStatus() + " " + u.getMessage());
                System.exit(1);
              }
            }));
      } catch (Exception e) {
        System.err.println("error: " + e);
        System.exit(1);
      }
      return;
    }
    if (args.length != 2) {
      System.err.println("usage: S1JGit <url> <repo-dir>");
      System.exit(2);
    }
    try (Git git = Git.open(new File(args[1]))) {
      git.fetch()
          .setRemote(args[0])
          .setRefSpecs(new RefSpec("+refs/heads/main:refs/remotes/origin/main"))
          .call();
    } catch (Exception e) {
      StringBuilder msg = new StringBuilder(e.toString());
      for (Throwable t = e.getCause(); t != null; t = t.getCause()) msg.append(": ").append(t);
      System.err.println("error: " + msg);
      System.exit(1);
    }
  }
}
