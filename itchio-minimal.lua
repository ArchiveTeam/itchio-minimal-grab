dofile("table_show.lua")
dofile("urlcode.lua")
dofile("strict.lua")
local urlparse = require("socket.url")
local luasocket = require("socket") -- Used to get sub-second time
local http = require("socket.http")
JSON = assert(loadfile "JSON.lua")()
local fun = require("fun")

local start_urls = JSON:decode(os.getenv("start_urls"))
local items_table = JSON:decode(os.getenv("item_names_table"))
local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local discovered_items = {}
local discovered_urls = {}
local current_item_type = nil
local current_item_value = nil
local next_start_url_index = 1

local do_retry = false
local redirects_level = 0

-- Downloads expire quickly, such that we can't just add them to URLs. So put an additional URL (http://archiveteam.invalid/) at the end of the wget initial URL list, and queue them all sequentially when we get there
local download_urls = {}
local got_to_time_constrained_url = false
local external_download_urls = {}

io.stdout:setvbuf("no") -- So prints are not buffered - http://lua.2524044.n2.nabble.com/print-stdout-and-flush-td6406981.html


function is_cdn_url(url)
  return url:match("itchio%-mirror.*cloudflarestorage%.com") or url:match("dl%.itch%.zone")
end

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local do_debug = false
print_debug = function(...)
  if do_debug then
    print(...)
  end
end
print_debug("This grab script is running in debug mode. You should not see this in production.")

local start_urls_inverted = {}
for _, v in pairs(start_urls) do
  start_urls_inverted[v] = true
end

-- Function to be called whenever an item's download ends.
end_of_item = function()
end

set_new_item = function(url)
  -- If next exists, and it matches the current
  if start_urls[next_start_url_index] and (urlparse.unescape(url) == urlparse.unescape(start_urls[next_start_url_index]))
    or (#start_urls == 1 and current_item_value == nil and urlparse.parse(start_urls[next_start_url_index]).authority == urlparse.parse(url).authority) then
    end_of_item()
    current_item_type = items_table[next_start_url_index][1]
    current_item_value = items_table[next_start_url_index][2]
    next_start_url_index = next_start_url_index + 1
    print_debug("Setting CIT to " .. current_item_type)
    print_debug("Setting CIV to " .. current_item_value)
  end
  assert(current_item_type)
  assert(current_item_value)
end


add_ignore = function(url)
  if url == nil then -- For recursion
    return
  end
  if downloaded[url] ~= true then
    downloaded[url] = true
  else
    return
  end
  add_ignore(string.gsub(url, "^https", "http", 1))
  add_ignore(string.gsub(url, "^http:", "https:", 1))
  add_ignore(string.match(url, "^ +([^ ]+)"))
  local protocol_and_domain_and_port = string.match(url, "^([a-zA-Z0-9]+://[^/]+)$")
  if protocol_and_domain_and_port then
    add_ignore(protocol_and_domain_and_port .. "/")
  end
  add_ignore(string.match(url, "^(.+)/$"))
end

for ignore in io.open("ignore-list", "r"):lines() do
  add_ignore(ignore)
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl, forced)
  --print_debug("Allowed on " .. url)
  assert(parenturl ~= nil)

  if start_urls_inverted[url] then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end
  
  if url == "https://cohost.org/sanqui/tagged/&" then
    -- Started timing out for no reason, causing tests to fail
    return false
  end
  
  if #url > 5000 and (url:match("data:[a-z]+/[a-zA-Z0-9%-%+_]+;base64")) then
    return false
  end
  
  local user, game = url:match("^https?://([^%.]+).itch%.io/([^/]+)")
  if user and user .. "/" .. game == current_item_value then
    return true
  end
  
  if url:match("^https?://[^%.]+%.itch%.io/.*/add%-to%-collection$") then
    return false
  end
  
  if url:match("^https?://[^%.]%.itch%.io/") or is_cdn_url(url) then
    return true
  end
  

  if forced then
    return true -- N.b. this function is bypassed by check() anyway
  else
    --print_debug("Renjecting", url)
    return false
  end
end



wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  --print_debug("DCP on " .. url)
  if downloaded[url] == true or addedtolist[url] == true then
    return false
  end
  
  if allowed(url, parent["url"]) then
    addedtolist[url] = true
    print_debug("DCP allowed " .. url)
    return true
  end

  return false
end



wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  local function check(urla, force)
    assert(urla:match("^https?://"))
    assert(not force or force == true) -- Don't accidentally put something else for force
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and (allowed(url_, origurl, force) or force) then
      print_debug("Queueing " .. url_)
      table.insert(urls, { url=url_, headers={["Accept-Language"]="en-US,en;q=0.5"}})
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end
  
  local function checknewurl(newurl)
    if not newurl then
      return
    end
    newurl = string.gsub(newurl, "\\$", "")
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check((string.gsub(newurl, ":////", "://")))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check((string.gsub(newurl, "\\", "")))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check_no_api(urlparse.absolute(url, "/" .. newurl))
    end
  end
  
  local function insane_url_extract(html)
    print_debug("IUE begin")
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    print_debug("IUE end")
  end

  local function load_html()
    if html == nil then
      html = read_file(file)
    end
    return html
  end
  
  local game_base_re = "^https?://[^/%.]+%.itch%.io/[^/%?]+"
  
  if current_item_type == "game" then
    local current_user, current_game = current_item_value:match("^([%l%d%-_]+)/([%w%-_]+)$")
    
    -- Queue all downloads from download buttons on the current page. With "suffix" on the end of the tell-me-the-cdn-url request.
    local function queue_download_buttons(suffix)
      local cxrf_token = load_html():match('<meta name="csrf_token" value="(.-)"')
      assert(cxrf_token)
      print_debug("CSRF token", cxrf_token)
      for download_id in load_html():gmatch('data%-upload_id="(%d+)"') do
        table.insert(download_urls, { url="https://" .. current_user .. ".itch.io/" .. current_game .. "/file/" .. download_id .. suffix, 
          post_data="csrf_token=" .. urlparse.escape(cxrf_token),
          headers={["Accept-Language"]="en-US,en;q=0.5"}})
      end
    end
    if url:match(game_base_re .. "$") then
      print_debug("Base case game:")
      assert(load_html():match('<style type="text/css" id="game_theme">') or load_html():match("We couldn&#039;t find your page"))
      if load_html():match("html_embed_%d+") or load_html():match("jar_drop") or load_html():match("Unity Web Player%. Install now!") or load_html():match("flash_notification") then
        print("Aborting", url, "because it has an embed; you do not need to report this")
        abortgrab = true -- Feel free to remove after the 1st or 2nd run
      end
      
      if load_html():match('class="button buy_btn"') then
        check(url .. "/purchase")
        check(url .. "/purchase?lightbox=true")
      end
      queue_download_buttons("?source=view_game&as_props=1")
    elseif url:match(game_base_re .. "/purchase$") then
      if load_html():match("direct_download_btn") then
        table.insert(urls, { url="https://" .. current_user .. ".itch.io/" .. current_game .. "/download_url", 
            method="POST",
            body_data="",
            headers={["Accept-Language"]="en-US,en;q=0.5"}})
      else
        assert(not load_html():match("No thanks, just take me to the downloads"))
      end
    elseif url:match(game_base_re .. "/download_url") then -- API request that gets made when you bypass pay-what-you-want
      local json = JSON:decode(load_html())
      local redir = json["url"]
      assert(redir:match(game_base_re .. "/download/[^/%?]+$"))
      check(redir)
    elseif url:match(game_base_re .. "/download/") then -- The page you get to after you bypass pay-what-you-want
      queue_download_buttons("?source=game_download&after_download_lightbox=1&as_props=1")
    
    -- Weird in-order thing here
    elseif url == "http://archiveteam.invalid/itch_end_of_normal_recurse" then
      assert(not got_to_time_constrained_url)
      got_to_time_constrained_url = true
      if #download_urls > 0 then
        table.insert(urls, download_urls[#download_urls])
        download_urls[#download_urls] = nil
      end
    elseif url:match(game_base_re .. "/file/") then
      assert(got_to_time_constrained_url)
      local json = JSON:decode(load_html())
      local dest = json["url"]:match("^([^#]+)")
      if not json["external"] then
        print_debug(dest, "should be CDN")
        assert(is_cdn_url(dest))
      else
        external_download_urls[dest] = true
      end
      check(dest, true)
    elseif is_cdn_url(url) or external_download_urls[url] then
      print_debug("Is finish chain, remaining:", JSON:encode(download_urls))
      assert(got_to_time_constrained_url)
      if #download_urls > 0 then
        table.insert(urls, download_urls[#download_urls])
        download_urls[#download_urls] = nil
      end
    end
  end
  
  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()


  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  assert(not (string.match(url["url"], "^https?://[^/]*google%.com/sorry") or string.match(url["url"], "^https?://consent%.google%.com/")))

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end


  -- External download chains
  if status_code >= 300 and status_code <= 399 and external_download_urls[url["url"]] and redirects_level < 5 then
    redirects_level = redirects_level + 1
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    print_debug("newloc is " .. newloc)
    if downloaded[newloc] == true or addedtolist[newloc] == true then
      tries = 0
      return wget.actions.EXIT
    else
      tries = 0
      print_debug("Following redirect to " .. newloc)
      external_download_urls[newloc] = true
      addedtolist[newloc] = true
      return wget.actions.NOTHING
    end
  end
  redirects_level = 0
  
  
    
  
  do_retry = false
  local maxtries = 3
  local url_is_essential = url["url"]:match("^https?://[^%.]+%.itch%.io/") or is_cdn_url(url["url"])

  -- Whitelist instead of blacklist status codes
  if status_code ~= 200 and status_code ~= 404 and not (status_code >= 300 and status_code < 400)
    and not (url["url"]:match("^http://archiveteam%.invalid/"))
    then
    print("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    do_retry = true
  end
  

  if do_retry then
    if tries >= maxtries then
      print("I give up...\n")
      tries = 0
      if not url_is_essential then
        return wget.actions.EXIT
      else
        print("Failed on an essential URL, aborting...")
        return wget.actions.ABORT
      end
    else
      sleep_time = math.floor(math.pow(2, tries))
      tries = tries + 1
    end
  end

  if do_retry and sleep_time > 0.001 then
    print("Sleeping " .. sleep_time .. "s")
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0
  return wget.actions.NOTHING
end



wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  end_of_item()
end

wget.callbacks.write_to_warc = function(url, http_stat)
  set_new_item(url["url"])
  if not (http_stat["statcode"] == 200 or http_stat["statcode"] == 404 or (http_stat["statcode"] >= 300 and http_stat["statcode"] < 400))
    and (url["url"]:match("^https?://[^%.]itch%.io/") or is_cdn_url(url["url"]))
    then
    print_debug("Not WTW")
    return false
  end
  return true
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  assert(got_to_time_constrained_url)
  assert(#download_urls == 0)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

