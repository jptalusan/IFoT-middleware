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
    console.log(res.unique_ID);
    console.log(res.data.task_id);
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.get_redis').on('click', function() {
  console.log('button clicked!');
  $.ajax({
    url: '/api/get_redis',
    data: { type: $(this).data('type') },
    method: 'POST'
  })
  .done((res) => {
    console.log(res);
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.flush_redis').on('click', function() {
  console.log('button clicked!');
  $.ajax({
    url: '/api/flush_redis',
    data: { type: $(this).data('type') },
    method: 'POST'
  })
  .done((res) => {
    // console.log(res.data.task_id);
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.test_redis').on('click', function() {
  console.log('button clicked!');
  $.ajax({
    url: '/api/set_redis',
    data: { type: $(this).data('type') },
    method: 'POST'
  })
  .done((res) => {
    // console.log(res.data.task_id);
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.checkQueue').on('click', function() {
  console.log('checking queue default');
  $.ajax({
    url: '/api/checkqueue',
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
    url: '/api/getallqueues',
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

  var artask = res.agg_running_tasks;
  var artask_length = Object.keys(artask).length;

  console.log(rtask_length, qtask_length, ftask_length);
  if (rtask_length + qtask_length + artask_length == 0) {
    $('#status').html('No tasks running, queued or finished...');
    return false;
  } else if (rtask_length + qtask_length + artask_length == 0) {
    return false;
  } else {
    return true;
  }
}

//probably race condition? i dont know how fast the queries are done but this loops every second
function getTaskStatus() {
  $.ajax({
    url: '/api/getmetas',
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

    var aftask = res.agg_finished_tasks;
    var aftask_length = Object.keys(aftask).length;

    if (aftask_length > 0) {
      console.log('Agg task fin len', aftask_length);
    }

    var aqtask = res.agg_queued_tasks;
    var aqtask_length = Object.keys(aqtask).length;

    if (aqtask_length > 0) {
      console.log('Agg task fin len', aqtask_length);
    }

    var artask = res.agg_running_tasks;
    var artask_length = Object.keys(artask).length;

    if (artask_length > 0) {
      console.log('Agg task fin len', artask_length);
    }

    //Running tasks
    for (var i = 0; i < rtask_length; ++i) {
      var t = Object.keys(rtask[i])[0];
      html += '<tr bgcolor="#337CFF">';
      html +=  `<td>${rtask[i][t].handled_time}</td>
                <td>${t.substring(0,8)}</td>
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
      html +=  `<td>${qtask[i][t].handled_time}</td>
                <td>${t.substring(0,8)}</td>
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
      html +=  `<td>${ftask[i][t].handled_time}</td>
                <td>${t.substring(0,8)}</td>
                <td>${ftask[i][t].handled_by}</td>
                <td>${ftask[i][t].progress}</td>
                <td>Finished</td>
                <td><a href="/api/task/default/${t}">Link</a></td>`;
      html += '</tr>';
    }

    //Agg Finished tasks
    for (var i = 0; i < aftask_length; i++) {
      var t = Object.keys(aftask[i])[0];
      html += '<tr bgcolor="#69E0D3">';
      html +=  `<td>${aftask[i][t].handled_time}</td>
                <td>${t.substring(0,8)}</td>
                <td>${aftask[i][t].handled_by}</td>
                <td>${aftask[i][t].progress}</td>
                <td>Finished</td>
                <td><a href="/api/task/aggregator/${t}">Link</a></td>`;
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
    url: '/api/getmetas',
    method: 'POST'
  })
  .done((res) => {
    if (window.isTaskWatchOngoing == false) {
      getTaskStatus();
      console.log(window.isTaskWatchOngoing);
    }
  })
  .fail((err) => {
    console.log(err);
  });
});

$('.test_nuts').on('click', function() {
  console.log('button clicked!');
  $.ajax({
    url: '/api/nuts_classify',
    data: { type: $(this).data('type') },
    method: 'POST'
  })
  .done((res) => {    
    console.log(res.unique_ID);
    console.log(res.data.task_id);
  })
  .fail((err) => {
    console.log(err);
  });
});