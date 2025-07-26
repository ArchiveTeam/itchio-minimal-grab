dofile("table_show.lua")
dofile("urlcode.lua")
dofile("strict.lua")
local urlparse = require("socket.url")
local luasocket = require("socket") -- Used to get sub-second time
local http = require("socket.http")
JSON = assert(loadfile "JSON.lua")()
CJSON = require "cjson"
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

io.stdout:setvbuf("no") -- So prints are not buffered - http://lua.2524044.n2.nabble.com/print-stdout-and-flush-td6406981.html

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

-- CJSON wrapper that turns it into JSON.lua format
-- Needed because in some cases I do "if [field]" and it'd take a lot of work to figure out if I'm checking for presence or checking for null
local function json_decode(s)
  local function convert(o)
    if type(o) == "table" then
      local new = {}
      for k, v in pairs(o) do
        if v ~= CJSON.null then
          new[k] = convert(v)
        end
      end
      return new
    else
      return o
    end
  end
  
  local function recursive_assert_equals(a, b)
    assert(type(a) == type(b))
    if type(a) == "table" then
      for k, v in pairs(a) do
        recursive_assert_equals(v, b[k])
      end
      for k, _ in pairs(b) do
        assert(a[k] ~= nil)
      end
    else
      assert(a == b, tostring(a) .. tostring(b))
    end
  end
  
  local out = convert(CJSON.decode(s))
  --recursive_assert_equals(out, JSON:decode(s))
  return out
end

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

discover_item = function(item_type, item_name)
  print_debug("Trying to discover " .. item_type .. ":" .. item_name)
  assert(item_type)
  assert(item_name)
  if item_type == "user" then
    assert(item_name:match("^" .. USERNAME_RE .. "$") or item_name:match("^" .. USERNAME_RE .. "%+%d+$"))
  end

  if not discovered_items[item_type .. ":" .. item_name] then
    print_debug("Queuing for discovery " .. item_type .. ":" .. item_name)
  end
  discovered_items[item_type .. ":" .. item_name] = true
end

discover_url = function(url)
  assert(url)
  --assert(url:match(":")) disabled for this project as potential garbage is sent here
  if url:match("\n") or not url:match(":") or #url > 500 or url:match("%s") then -- Garbage
    return
  end
  if not discovered_urls[url] then
    print_debug("Discovering for #// " .. url)
    discovered_urls[url] = true
  end
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
  print_debug("Allowed on " .. url)
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
  

  if forced then
    return true -- N.b. this function is bypassed by check() anyway
  else
    discover_url(url)
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
      local link_expect_html = nil
      table.insert(urls, { url=url_, headers={["Accept-Language"]="en-US,en;q=0.5"}, link_expect_html=link_expect_html})
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


  -- Handle redirects not in download chains
  if status_code >= 300 and status_code <= 399 and (
      (redirects_level > 0 and redirects_level < 5)
    ) then
    redirects_level = redirects_level + 1
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    print_debug("newloc is " .. newloc)
    if downloaded[newloc] == true or addedtolist[newloc] == true then
      tries = 0
      return wget.actions.EXIT
    else
      tries = 0
      print_debug("Following redirect to " .. newloc)
      assert(not (string.match(newloc, "^https?://[^/]*google%.com/sorry") or string.match(newloc, "^https?://consent%.google%.com/")))
      assert(not string.match(url["url"], "^https?://drive%.google%.com/file/d/.*/view$")) -- If this is a redirect, it will mess up initialization of file: items
      assert(not string.match(url["url"], "^https?://drive%.google%.com/drive/folders/[0-9A-Za-z_%-]+/?$")) -- Likewise for folder:

      addedtolist[newloc] = true
      return wget.actions.NOTHING
    end
  end
  redirects_level = 0
    
  
  do_retry = false
  local maxtries = 0
  local url_is_essential = true

  -- Whitelist instead of blacklist status codes
  if status_code ~= 200 and status_code ~= 404
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


local send_binary = function(to_send, key)
  local tries = 0
  while tries < 10 do
    local body, code, headers, status = http.request(
            "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
            to_send
    )
    if code == 200 or code == 409 then
      break
    end
    print("Failed to submit discovered URLs." .. tostring(code) .. " " .. tostring(body)) -- From arkiver https://github.com/ArchiveTeam/vlive-grab/blob/master/vlive.lua
    os.execute("sleep " .. math.floor(math.pow(2, tries)))
    tries = tries + 1
  end
  if tries == 10 then
    error("Failed to send binary")
  end
end

-- Taken verbatim from previous projects I've done'
local queue_list_to = function(list, key)
  assert(key)
  if do_debug then
    for item, _ in pairs(list) do
      assert(string.match(item, ":"))
      assert(not fun.iter(item):any(function(b) return b == "\0" end))
      print("Would have sent discovered item " .. item)
    end
  else
    local to_send = nil
    for item, _ in pairs(list) do
      assert(string.match(item, ":")) -- Message from EggplantN, #binnedtray (search "colon"?)
      assert(not fun.iter(item):any(function(b) return b == "\0" end))
      if to_send == nil then
        to_send = item
      else
        to_send = to_send .. "\0" .. item
      end
      print("Queued " .. item)

      if #to_send > 1500 then
        send_binary(to_send .. "\0", key)
        to_send = ""
      end
    end

    if to_send ~= nil and #to_send > 0 then
      send_binary(to_send .. "\0", key)
    end
  end
end


wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  end_of_item()
  queue_list_to(discovered_items, "cohost-wewri2htv6akk1ij")
  queue_list_to(discovered_urls, "urls-eucpu0yrat3fsajp")
end

wget.callbacks.write_to_warc = function(url, http_stat)
  set_new_item(url["url"])
  if http_stat["statcode"] ~= 200 and http_stat["statcode"] ~= 404 then
    print_debug("Not WTW")
    return false
  end
  return true
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

