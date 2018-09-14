var isTaskWatchOngoing = false;

$( document ).ready(() => {
  console.log('Sanity check!');
});

$('.btn').on('click', function() {
  console.log('button clicked!');
  $.ajax({
    url: '/tasks',
    data: { type: $(this).data('type') },
    method: 'POST'
  })
  .done((res) => {
    console.log(res.data.task_id);
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.checkQueue').on('click', function() {
  console.log('checking queue default');
  $.ajax({
    url: '/checkqueue',
    data: { queue: 'queue' },
    method: 'POST'
  })
  .done((res) => {
    var data = JSON.parse(res);
    console.log(data["response"]["job_count"]);
    console.log(data["response"]["jobs"]);
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.getAllQueues').on('click', function() {
  $.ajax({
    url: '/getallqueues',
    method: 'POST'
  })
  .done((res) => {
    console.log(res);
    console.log(res.running.running_tasks_ids[0]);
    /*
    Loop through all IDs 
      //create html file <tr><td><td>....
      if finished:
        get results alongside the task/<task_id> json
      else if queued
        get current status
      else: //if failed
        inform also in table

      prepend html file to #tasks class
      or gather all html string and then make one big string, and then replace the tasks
    */
  })
  .fail((err) => {
    console.log(err);
  });
});

function checkIfShouldQuery(res) {
  var rtask = res.running_tasks;
  var rtask_length = Object.keys(rtask).length;
  var qtask = res.queued_tasks;
  var qtask_length = Object.keys(qtask).length;
  var ftask = res.finished_tasks;
  var ftask_length = Object.keys(ftask).length;
  console.log(rtask_length, qtask_length, ftask_length);
  if (rtask_length + qtask_length + ftask_length == 0) {
    $('#status').html('No tasks running, queued or finished...');
    return false;
  } else if (rtask_length + qtask_length == 0) {
    return false;
  } else {
    return true;
  }
}

function getTaskStatus() {
  $.ajax({
    url: '/getmetas',
    method: 'POST'
  })
  .done((res) => {
    var html = '';
    
    var rtask = res.running_tasks;
    var rtask_length = Object.keys(rtask).length;
    var qtask = res.queued_tasks;
    var qtask_length = Object.keys(qtask).length;
    var ftask = res.finished_tasks;
    var ftask_length = Object.keys(ftask).length;

    //Running tasks
    for (var i = 0; i < rtask_length; ++i) {
      var t = Object.keys(rtask[i])[0];
      html += '<tr bgcolor="#337CFF">';
      html +=  `<td>${t.substring(0,8)}</td>
                <td>${rtask[i][t].handled_by}</td>
                <td>${rtask[i][t].progress}</td>
                <td>Running</td>
                <td>${rtask[i][t].result}</td>`;
      html += '</tr>';
    }

    //Queued tasks
    for (var i = 0; i < qtask_length; ++i) {
      var t = Object.keys(qtask[i])[0];
      html += '<tr bgcolor="#E4FF33">';
      html +=  `<td>${t.substring(0,8)}</td>
                <td>${qtask[i][t].handled_by}</td>
                <td>${qtask[i][t].progress}</td>
                <td>Queued</td>
                <td>${qtask[i][t].result}</td>`;
      html += '</tr>';
    }

    //Finished tasks
    for (var i = 0; i < ftask_length; i++) {
      var t = Object.keys(ftask[i])[0];
      html += '<tr bgcolor="#33FF55">';
      html +=  `<td>${t.substring(0,8)}</td>
                <td>${ftask[i][t].handled_by}</td>
                <td>${ftask[i][t].progress}</td>
                <td>Finished</td>
                <td><a href="/task/${t}">Link</a></td>`;
      html += '</tr>';
    }

    
    $('#tasks').html(html);

    if (checkIfShouldQuery(res) == false) {
      console.log('Done checking!');
      window.isTaskWatchOngoing = false;
      return false;
    } else {
      setTimeout(function() {
        window.isTaskWatchOngoing = true;
        getTaskStatus(res);
        $('#tasks').html(html);
        console.log('Checking again in 1 sec!');
      }, 1000);
    }

    return true;
  })
  .fail((err) => {
    console.log(err)
  });
}

//i can add the get metas here?
$('.getmetas').on('click', function() {
  $.ajax({
    url: '/getmetas',
    method: 'POST'
  })
  .done((res) => {
    // console.log(res);
    // console.log(res.finished_tasks);
    if (window.isTaskWatchOngoing == false) {
      getTaskStatus();
      console.log(window.isTaskWatchOngoing);
    }
    // Loop through all IDs 
    // //create html file <tr><td><td>....
    // if finished:
    //   get results alongside the task/<task_id> json
    // else if queued
    //   get current status
    // else: //if failed
    //   inform also in table
    // prepend html file to #tasks class
    // or gather all html string and then make one big string, and then replace the tasks
  })
  .fail((err) => {
    console.log(err);
  });
});
