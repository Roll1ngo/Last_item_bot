chrome.runtime.onInstalled.addListener(() => {
  chrome.declarativeNetRequest.updateDynamicRules({
    addRules: [
      {
        id: 1,
        priority: 1,
        action: {
          type: 'modifyHeaders',
          responseHeaders: [
            { header: 'Authorization', operation: 'remove' }
          ]
        },
        condition: {
          urlFilter: '*://sls.g2g.com/offer/search?seo_term*',
          resourceTypes: ['xmlhttprequest']
        }
      }
    ],
    removeRuleIds: [1]
  });
});

chrome.webRequest.onBeforeSendHeaders.addListener(
  function(details) {
    if (details.method === "GET" && details.url.includes("sls.g2g.com/offer/search?seo_term")) {
      let authHeader = details.requestHeaders.find(header => header.name.toLowerCase() === "authorization");
      if (authHeader) {
        let token = authHeader.value;
        let config = {
          Authorization: token
        };
        saveConfig(config);
      }
    }
    return {requestHeaders: details.requestHeaders};
  },
  {urls: ["<all_urls>"]},
  ["requestHeaders"]
);

function saveConfig(config) {
  chrome.storage.local.set({authConfig: config}, function() {
    console.log('Authorization token saved.');
  });

  let configJson = JSON.stringify(config);
  let dataUrl = "data:application/json;base64," + btoa(configJson);

  chrome.downloads.download({
    url: dataUrl,
    filename: "authorization.json",
    saveAs: false // Автоматическое сохранение с предложенным именем файла
  });
}

// Функция для обновления вкладки с https://www.g2g.com каждые 10 минут
function refreshG2GTab() {
  chrome.tabs.query({}, function(tabs) {
    for (let tab of tabs) {
      if (tab.url && tab.url.includes("https://www.g2g.com")) {
        chrome.tabs.reload(tab.id);
      }
    }
  });
}

// Устанавливаем таймер для обновления вкладки каждые 10 минут
// setInterval(refreshG2GTab, 1 * 60 * 1000);
