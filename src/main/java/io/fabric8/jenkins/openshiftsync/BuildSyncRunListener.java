/**
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fabric8.jenkins.openshiftsync;

import com.cloudbees.workflow.rest.external.AtomFlowNodeExt;
import com.cloudbees.workflow.rest.external.FlowNodeExt;
import com.cloudbees.workflow.rest.external.RunExt;
import com.cloudbees.workflow.rest.external.StageNodeExt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hudson.Extension;
import hudson.PluginManager;
import hudson.console.ConsoleNote;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.model.listeners.RunListener;
import hudson.triggers.SafeTimerTask;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.openshift.api.model.Build;
import jenkins.model.Jenkins;
import jenkins.util.Timer;

import org.apache.commons.httpclient.HttpStatus;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.kohsuke.stapler.DataBoundConstructor;

import javax.annotation.Nonnull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.fabric8.jenkins.openshiftsync.Constants.OPENSHIFT_ANNOTATIONS_JENKINS_BUILD_URI;
import static io.fabric8.jenkins.openshiftsync.Constants.OPENSHIFT_ANNOTATIONS_JENKINS_LOG_URL;
import static io.fabric8.jenkins.openshiftsync.Constants.OPENSHIFT_ANNOTATIONS_JENKINS_STATUS_JSON;
import static io.fabric8.jenkins.openshiftsync.JenkinsUtils.maybeScheduleNext;
import static io.fabric8.jenkins.openshiftsync.OpenShiftUtils.formatTimestamp;
import static io.fabric8.jenkins.openshiftsync.OpenShiftUtils.getAuthenticatedOpenShiftClient;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

/**
 * Listens to Jenkins Job build {@link Run} start and stop then ensure there's a suitable {@link Build} object in
 * OpenShift thats updated correctly with the current status, logsURL and metrics
 */
@Extension
public class BuildSyncRunListener extends RunListener<Run> {
  private static final Logger logger = Logger.getLogger(BuildSyncRunListener.class.getName());

  private long pollPeriodMs = 1000;
  private String namespace;

  private transient Set<Run> runsToPoll = new CopyOnWriteArraySet<>();
  private transient Map<String, List<String>> logsForRuns = new HashMap<String, List<String>>();
  private transient Map<String, List<String>> logIndexesForRuns = new HashMap<String, List<String>>();

  private transient AtomicBoolean timerStarted = new AtomicBoolean(false);

  public BuildSyncRunListener() {}

  @DataBoundConstructor
  public BuildSyncRunListener(long pollPeriodMs) {
    this.pollPeriodMs = pollPeriodMs;
  }

  /**
   * Joins all the given strings, ignoring nulls so that they form a URL with / between the paths without a // if the
   * previous path ends with / and the next path starts with / unless a path item is blank
   *
   * @param strings the sequence of strings to join
   * @return the strings concatenated together with / while avoiding a double // between non blank strings.
   */
  public static String joinPaths(String... strings) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strings.length; i++) {
      sb.append(strings[i]);
      if (i < strings.length - 1) {
        sb.append("/");
      }
    }
    String joined = sb.toString();

    // And normalize it...
    return joined
      .replaceAll("/+", "/")
      .replaceAll("/\\?", "?")
      .replaceAll("/#", "#")
      .replaceAll(":/", "://");
  }

  @Override
  public synchronized void onStarted(Run run, TaskListener listener) {
    if (shouldPollRun(run)) {
      try {
        BuildCause cause = (BuildCause) run.getCause(BuildCause.class);
        if (cause != null) {
          // TODO This should be a link to the OpenShift console.
          run.setDescription(cause.getShortDescription());
        }
      } catch (IOException e) {
        logger.log(WARNING, "Cannot set build description: " + e);
      }
      if (runsToPoll.add(run)) {
        logger.info("starting polling build " + run.getUrl());
      }
      logsForRuns.put(run.getFullDisplayName(), new ArrayList<String>());
      logIndexesForRuns.put(run.getFullDisplayName(), new ArrayList<String>());
      checkTimerStarted();
    } else {
      logger.fine("not polling polling build " + run.getUrl() + " as its not a WorkflowJob");
    }
    super.onStarted(run, listener);
  }

  protected void checkTimerStarted() {
    if (timerStarted.compareAndSet(false, true)) {
      Runnable task = new SafeTimerTask() {
        @Override
        protected void doRun() throws Exception {
          pollLoop();
        }
      };
      Timer.get().scheduleAtFixedRate(task, pollPeriodMs, pollPeriodMs, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public synchronized void onCompleted(Run run, @Nonnull TaskListener listener) {
    if (shouldPollRun(run)) {
      runsToPoll.remove(run);
      pollRun(run);
      logger.info("onCompleted " + run.getUrl());
      maybeScheduleNext(((WorkflowRun) run).getParent());
    }
    addRunLogs(run);
    // onFinalized always gets called ... clean up maps then, do one more pass at run logs
    super.onCompleted(run, listener);
  }

  @Override
  public synchronized void onDeleted(Run run) {
    if (shouldPollRun(run)) {
      runsToPoll.remove(run);
      pollRun(run);
      logger.info("onDeleted " + run.getUrl());
      maybeScheduleNext(((WorkflowRun) run).getParent());
    }
    addRunLogs(run);
    logsForRuns.remove(run.getFullDisplayName());
    logIndexesForRuns.remove(run.getFullDisplayName());
    super.onDeleted(run);
  }

  @Override
  public synchronized void onFinalized(final Run run) {
    if (shouldPollRun(run)) {
      runsToPoll.remove(run);
      pollRun(run);
      logger.info("onFinalized " + run.getUrl());
    }
    addRunLogs(run);
    Runnable task = new SafeTimerTask() {
        @Override
        protected void doRun() throws Exception {
            logger.info("GGM on last log pull, then delete annotations");
            addRunLogs(run);
            deleteAnnotations(run);
            logsForRuns.remove(run.getFullDisplayName());
            logIndexesForRuns.remove(run.getFullDisplayName());
        }
      };
    Timer.get().schedule(task, 5, TimeUnit.SECONDS);
    super.onFinalized(run);
  }

  protected synchronized void pollLoop() {
    for (Run run : runsToPoll) {
      pollRun(run);
    }
  }

  protected synchronized void pollRun(Run run) {
    if (!(run instanceof WorkflowRun)) {
      throw new IllegalStateException("Cannot poll a non-workflow run");
    }

    RunExt wfRunExt = RunExt.create((WorkflowRun) run);

    try {
      upsertBuild(run, wfRunExt);
    } catch (KubernetesClientException e) {
      if (e.getCode() == HttpStatus.SC_UNPROCESSABLE_ENTITY) {
        runsToPoll.remove(run);
        logsForRuns.remove(run.getFullDisplayName());
        logIndexesForRuns.remove(run.getFullDisplayName());
        logger.log(WARNING, "Cannot update status: {0}", e.getMessage());
        return;
      }
      throw e;
    }
  }

  private void upsertBuild(Run run, RunExt wfRunExt) {
    if (run == null) {
      return;
    }

    BuildCause cause = (BuildCause) run.getCause(BuildCause.class);
    if (cause == null) {
      return;
    }

    String rootUrl = OpenShiftUtils.getJenkinsURL(getAuthenticatedOpenShiftClient(), cause.getNamespace());
    String buildUrl = joinPaths(rootUrl, run.getUrl());
    String logsUrl = joinPaths(buildUrl, "/consoleText");
    String logsConsoleUrl = joinPaths(buildUrl, "/console");
    String logsBlueOceanUrl = null;
    try {
        // while we support Jenkins v1 (it is being deprecated in openshift 3.6), need to get at
        // blueocean plugins via reflection;
        // On those plugins specifically, there are utility functions in the blueocean-dashboard plugin which construct 
        // this entire URI; however, attempting to pull that in as a maven dependency was untenable from an injected test perspective;
        // the blueocean-rest-impl plugin was possible though, and the organization piece was in fact the only one
        // that was missing for use to construct the entire URL manually.
        // But with reflection, we can leverage the blueocean-dashboard logic.  Doing so here, but have left the 
        // blueocean-rest-impl plugin usage as a comment for future reference if we move off of reflection.
        Jenkins jenkins = Jenkins.getInstance();
        // NOTE, the excessive null checking is to keep `mvn findbugs:gui` quiet
        if (jenkins != null) {
            PluginManager pluginMgr = jenkins.getPluginManager();
            if (pluginMgr != null) {
                ClassLoader cl = pluginMgr.uberClassLoader;
                if (cl != null) {
                    Class weburlbldr = cl.loadClass("io.jenkins.blueocean.BlueOceanWebURLBuilder");
                    Method toBlueOceanURLMethod = weburlbldr.getMethod("toBlueOceanURL", hudson.model.ModelObject.class);
                    Object blueOceanURI = toBlueOceanURLMethod.invoke(null, run);
                    logsBlueOceanUrl = joinPaths(rootUrl, blueOceanURI.toString());
                }
            }
        }
        /*
        Class factoryClass = cl.loadClass("io.jenkins.blueocean.service.embedded.rest.BluePipelineFactory");
        Method resolveMethod = factoryClass.getMethod("resolve", hudson.model.Item.class);
        Object resolveReturn = resolveMethod.invoke(null, run.getParent());
        Method getOrg = resolveReturn.getClass().getMethod("getOrganization", null);
        Object org = getOrg.invoke(resolveReturn, null);
        logsBlueOceanUrl = joinPaths(rootUrl, "blue", "organizations", URLEncoder.encode(org.toString(), "UTF-8"), 
                URLEncoder.encode(run.getParent().getName(), "UTF-8"), "detail",
                URLEncoder.encode(run.getParent().getName(), "UTF-8"), Integer.toString(run.getNumber()), "pipeline");
         */
    } catch (Throwable t) {
        if (logger.isLoggable(Level.FINE))
            logger.log(Level.FINE, "upsertBuild", t);
    }

    if (!wfRunExt.get_links().self.href.matches("^https?://.*$")) {
      wfRunExt.get_links().self.setHref(joinPaths(rootUrl, wfRunExt.get_links().self.href));
    }
    for (StageNodeExt stage : wfRunExt.getStages()) {
      FlowNodeExt.FlowNodeLinks links = stage.get_links();
      if (!links.self.href.matches("^https?://.*$")) {
        links.self.setHref(joinPaths(rootUrl, links.self.href));
      }
      if (links.getLog() != null && !links.getLog().href.matches("^https?://.*$")) {
        links.getLog().setHref(joinPaths(rootUrl, links.getLog().href));
      }
      for (AtomFlowNodeExt node : stage.getStageFlowNodes()) {
        FlowNodeExt.FlowNodeLinks nodeLinks = node.get_links();
        if (!nodeLinks.self.href.matches("^https?://.*$")) {
          nodeLinks.self.setHref(joinPaths(rootUrl, nodeLinks.self.href));
        }
        if (nodeLinks.getLog() != null && !nodeLinks.getLog().href.matches("^https?://.*$")) {
          nodeLinks.getLog().setHref(joinPaths(rootUrl, nodeLinks.getLog().href));
        }
      }
    }

    String json;
    try {
      json = new ObjectMapper().writeValueAsString(wfRunExt);
    } catch (JsonProcessingException e) {
      logger.log(SEVERE, "Failed to serialize workflow run. " + e, e);
      return;
    }

    String phase = runToBuildPhase(run);

    long started = getStartTime(run);
    String startTime = null;
    String completionTime = null;
    if (started > 0) {
      startTime = formatTimestamp(started);

      long duration = getDuration(run);
      if (duration > 0) {
        completionTime = formatTimestamp(started + duration);
      }
    }
    
    addRunLogs(run);
    
    logger.log(FINE, "Patching build {0}/{1}: setting phase to {2}", new Object[]{cause.getNamespace(), cause.getName(), phase});
    try {
      getAuthenticatedOpenShiftClient().builds().inNamespace(cause.getNamespace()).withName(cause.getName()).edit()
        .editMetadata()
        .addToAnnotations(OPENSHIFT_ANNOTATIONS_JENKINS_STATUS_JSON, json)
        .addToAnnotations(OPENSHIFT_ANNOTATIONS_JENKINS_BUILD_URI, buildUrl)
        .addToAnnotations(OPENSHIFT_ANNOTATIONS_JENKINS_LOG_URL, logsUrl)
        .addToAnnotations(Constants.OPENSHIFT_ANNOTATIONS_JENKINS_CONSOLE_LOG_URL, logsConsoleUrl)
        .addToAnnotations(Constants.OPENSHIFT_ANNOTATIONS_JENKINS_BLUEOCEAN_LOG_URL, logsBlueOceanUrl)
        .endMetadata()
        .editStatus()
        .withPhase(phase)
        .withStartTimestamp(startTime)
        .withCompletionTimestamp(completionTime)
        .endStatus()
        .done();
    } catch (KubernetesClientException e) {
      if (HTTP_NOT_FOUND == e.getCode()) {
        runsToPoll.remove(run);
        logsForRuns.remove(run.getFullDisplayName());
        logIndexesForRuns.remove(run.getFullDisplayName());
      } else {
        throw e;
      }
    }
  }
  
  private void addRunLogs(Run run) {
      BuildCause cause = (BuildCause) run.getCause(BuildCause.class);
      if (cause == null) {
        return;
      }
      List<String> existingLogs = logsForRuns.get(run.getFullDisplayName());
      List<String> indexes = logIndexesForRuns.get(run.getFullDisplayName());
      List<String> newLogs = new ArrayList<String>();
      String logString = "";
      String logIndex = Long.toString(System.nanoTime());
      indexes.add(logIndex);
      // FYI, tried hudson.model.Run.getLog(int) and it just didn't work real well
      if (existingLogs != null) {
          try {
              Reader rdr = run.getLogReader();
              BufferedReader buff = new BufferedReader(rdr);
              String line = null;
              while ((line = buff.readLine()) != null) {
                  line = ConsoleNote.removeNotes(line);
                  if (!existingLogs.contains(line)) {
                      newLogs.add(line);
                      logString = logString + line + "\n";
                  }
              }
              existingLogs.addAll(newLogs);
              logsForRuns.put(run.getFullDisplayName(), existingLogs);
              getOpenShiftClient().builds().inNamespace(cause.getNamespace()).withName(cause.getName()).edit()
              .editMetadata()
              .addToAnnotations(Constants.OPENSHIFT_ANNOTATIONS_JENKINS_LOG_CONTENT_RAWDATA_PREFIX + logIndex, logString)
              .endMetadata()
              .done();
          } catch (IOException e1) {
              logger.log(Level.WARNING, "addRunLogs", e1);
          } catch (KubernetesClientException e) {
              if (HTTP_NOT_FOUND == e.getCode()) {
                  runsToPoll.remove(run);
                  logsForRuns.remove(run.getFullDisplayName());
              } else {
                  throw e;
              }
          }
      }
  }
  
  private void deleteAnnotations(Run run) {
      BuildCause cause = (BuildCause) run.getCause(BuildCause.class);
      if (cause == null) {
        logger.info("deleteAnnotation cause null ??");
        return;
      }
      List<String> indexes = logIndexesForRuns.get(run.getFullDisplayName());
      for (String logIndex : indexes) {
          try {
              getOpenShiftClient().builds().inNamespace(cause.getNamespace()).withName(cause.getName()).edit()
              .editMetadata()
              .removeFromAnnotations(Constants.OPENSHIFT_ANNOTATIONS_JENKINS_LOG_CONTENT_RAWDATA_PREFIX + logIndex)
              .endMetadata()
              .done();
          } catch (KubernetesClientException e) {
              if (HTTP_NOT_FOUND == e.getCode()) {
                  runsToPoll.remove(run);
                  logsForRuns.remove(run.getFullDisplayName());
              } else {
                  throw e;
              }
          }
      }
  }
  
  private long getStartTime(Run run) {
    return run.getStartTimeInMillis();
  }

  private long getDuration(Run run) {
    return run.getDuration();
  }

  private String runToBuildPhase(Run run) {
    if (run != null && !run.hasntStartedYet()) {
      if (run.isBuilding()) {
        return BuildPhases.RUNNING;
      } else {
        Result result = run.getResult();
        if (result != null) {
          if (result.equals(Result.SUCCESS)) {
            return BuildPhases.COMPLETE;
          } else if (result.equals(Result.ABORTED)) {
            return BuildPhases.CANCELLED;
          } else if (result.equals(Result.FAILURE)) {
            return BuildPhases.FAILED;
          } else if (result.equals(Result.UNSTABLE)) {
            return BuildPhases.FAILED;
          } else {
            return BuildPhases.PENDING;
          }
        }
      }
    }
    return BuildPhases.NEW;
  }

  /**
   * Returns true if we should poll the status of this run
   *
   * @param run the Run to test against
   * @return true if the should poll the status of this build run
   */
  protected boolean shouldPollRun(Run run) {
    return run instanceof WorkflowRun && run.getCause(BuildCause.class) != null;
  }
}
